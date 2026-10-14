import pickle
from importlib.metadata import entry_points

import boto3
import fsspec
import pytest
from moto import mock_aws

from pfio.fsspec import (PfioFileFileSystem, PfioHdfsFileSystem,
                         PfioS3FileSystem, register)


@pytest.fixture(autouse=True)
def _clear_fsspec_cache():
    # fsspec caches filesystem instances by class + storage_options. Clear it
    # around every test so a cached S3 client cannot leak across moto mocks.
    for cls in (PfioFileFileSystem, PfioS3FileSystem, PfioHdfsFileSystem):
        cls.clear_instance_cache()
    yield
    for cls in (PfioFileFileSystem, PfioS3FileSystem, PfioHdfsFileSystem):
        cls.clear_instance_cache()


# ---------------------------------------------------------------------------
# pfio-file
# ---------------------------------------------------------------------------

def test_file_roundtrip(tmp_path):
    fs = fsspec.filesystem("pfio-file")
    assert isinstance(fs, PfioFileFileSystem)

    target = str(tmp_path / "a.txt")
    with fs.open(target, "wb") as f:
        f.write(b"hello")

    assert fs.cat(target) == b"hello"
    assert fs.exists(target)
    assert fs.isfile(target)
    assert not fs.isdir(target)


def test_file_open_url(tmp_path):
    target = tmp_path / "b.txt"
    with fsspec.open("pfio-file://" + str(target), "wb") as f:
        f.write(b"world")

    with fsspec.open("pfio-file://" + str(target), "rb") as f:
        assert f.read() == b"world"


def test_file_info_and_ls(tmp_path):
    fs = fsspec.filesystem("pfio-file")
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.txt").write_bytes(b"12345")

    info = fs.info(str(tmp_path / "a.txt"))
    assert info["type"] == "file"
    assert info["size"] == 5
    assert info["name"] == str(tmp_path / "a.txt")

    dinfo = fs.info(str(tmp_path / "sub"))
    assert dinfo["type"] == "directory"
    assert dinfo["size"] == 0

    listing = fs.ls(str(tmp_path), detail=True)
    names = {e["name"]: e["type"] for e in listing}
    assert names[str(tmp_path / "a.txt")] == "file"
    assert names[str(tmp_path / "sub")] == "directory"

    # detail=False returns full paths
    names = fs.ls(str(tmp_path), detail=False)
    assert str(tmp_path / "a.txt") in names
    assert str(tmp_path / "sub") in names


def test_file_ls_on_file(tmp_path):
    fs = fsspec.filesystem("pfio-file")
    target = str(tmp_path / "a.txt")
    (tmp_path / "a.txt").write_bytes(b"12345")

    # fsspec contract: ls() of a file yields a single-element listing.
    listing = fs.ls(target, detail=True)
    assert len(listing) == 1
    assert listing[0]["name"] == target
    assert listing[0]["type"] == "file"
    assert fs.ls(target, detail=False) == [target]


def test_file_makedirs_and_rm(tmp_path):
    fs = fsspec.filesystem("pfio-file")
    d = str(tmp_path / "x" / "y")
    fs.makedirs(d, exist_ok=True)
    assert fs.isdir(d)

    target = str(tmp_path / "x" / "y" / "f.txt")
    with fs.open(target, "wb") as f:
        f.write(b"z")
    fs.rm(target)
    assert not fs.exists(target)


# ---------------------------------------------------------------------------
# pfio-s3
# ---------------------------------------------------------------------------

@pytest.fixture
def s3_mock():
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="test-bucket")
        client.create_bucket(Bucket="other-bucket")
        yield


def test_s3_roundtrip(s3_mock):
    fs = fsspec.filesystem("pfio-s3")
    assert isinstance(fs, PfioS3FileSystem)

    with fs.open("pfio-s3://test-bucket/dir/a.txt", "wb") as f:
        f.write(b"data")

    assert fs.cat("test-bucket/dir/a.txt") == b"data"
    assert fs.exists("test-bucket/dir/a.txt")
    assert fs.isfile("test-bucket/dir/a.txt")


def test_s3_ls_full_path(s3_mock):
    fs = fsspec.filesystem("pfio-s3")
    with fs.open("pfio-s3://test-bucket/dir/a.txt", "wb") as f:
        f.write(b"data")
    with fs.open("pfio-s3://test-bucket/dir/sub/b.txt", "wb") as f:
        f.write(b"data")

    listing = fs.ls("test-bucket/dir", detail=True)
    by_name = {e["name"]: e for e in listing}
    assert "test-bucket/dir/a.txt" in by_name
    assert by_name["test-bucket/dir/a.txt"]["type"] == "file"
    # the common prefix shows up as a directory
    assert "test-bucket/dir/sub" in by_name
    assert by_name["test-bucket/dir/sub"]["type"] == "directory"


def test_s3_ls_on_file(s3_mock):
    fs = fsspec.filesystem("pfio-s3")
    with fs.open("pfio-s3://test-bucket/dir/a.txt", "wb") as f:
        f.write(b"data")

    listing = fs.ls("test-bucket/dir/a.txt", detail=True)
    assert len(listing) == 1
    assert listing[0]["name"] == "test-bucket/dir/a.txt"
    assert listing[0]["type"] == "file"
    assert fs.ls("test-bucket/dir/a.txt", detail=False) \
        == ["test-bucket/dir/a.txt"]


def test_s3_isdir_and_info_directory(s3_mock):
    fs = fsspec.filesystem("pfio-s3")
    with fs.open("pfio-s3://test-bucket/dir/a.txt", "wb") as f:
        f.write(b"data")

    assert fs.isdir("test-bucket/dir")
    # S3PrefixStat has size == -1; it must be normalized to a directory info.
    info = fs.info("test-bucket/dir")
    assert info["type"] == "directory"
    assert info["size"] == 0


def test_s3_multiple_buckets_cached(s3_mock):
    fs = fsspec.filesystem("pfio-s3")
    with fs.open("pfio-s3://test-bucket/a.txt", "wb") as f:
        f.write(b"a")
    with fs.open("pfio-s3://other-bucket/b.txt", "wb") as f:
        f.write(b"b")

    assert fs.cat("test-bucket/a.txt") == b"a"
    assert fs.cat("other-bucket/b.txt") == b"b"
    # one pfio S3 instance lazily created per bucket
    assert set(fs._fs_cache.keys()) == {"test-bucket", "other-bucket"}


def test_s3_pickle(s3_mock):
    fs = fsspec.filesystem("pfio-s3")
    with fs.open("pfio-s3://test-bucket/a.txt", "wb") as f:
        f.write(b"hello")

    data = pickle.dumps(fs)
    # Drop the cached instance so unpickling rebuilds from storage_options
    # (as it would in a separate process), rather than hitting fsspec's
    # in-process instance cache and returning the very same object.
    PfioS3FileSystem.clear_instance_cache()
    fs2 = pickle.loads(data)
    assert fs2 is not fs
    # internal pfio instances are not part of the pickled state
    assert fs2._fs_cache == {}
    assert fs2.cat("test-bucket/a.txt") == b"hello"


# ---------------------------------------------------------------------------
# Instance cache / class separation
# ---------------------------------------------------------------------------

def test_instance_cache_same_options():
    a = fsspec.filesystem("pfio-file")
    b = fsspec.filesystem("pfio-file")
    assert a is b


def test_protocols_do_not_collide():
    f = fsspec.filesystem("pfio-file")
    s = fsspec.filesystem("pfio-s3")
    assert type(f) is PfioFileFileSystem
    assert type(s) is PfioS3FileSystem
    assert f is not s


# ---------------------------------------------------------------------------
# Entry points / registration
# ---------------------------------------------------------------------------

def test_entry_points_registered():
    eps = entry_points(group="fsspec.specs")
    names = {e.name for e in eps}
    if not {"pfio-file", "pfio-s3", "pfio-hdfs"} <= names:
        pytest.skip("entry points not visible (package not installed)")
    assert fsspec.get_filesystem_class("pfio-file") is PfioFileFileSystem
    assert fsspec.get_filesystem_class("pfio-s3") is PfioS3FileSystem
    assert fsspec.get_filesystem_class("pfio-hdfs") is PfioHdfsFileSystem


def test_register_helper():
    from fsspec.registry import _registry as reg
    original = reg.get("s3")
    try:
        register(s3=True)
        assert fsspec.get_filesystem_class("s3") is PfioS3FileSystem
    finally:
        # restore the registry to avoid leaking into other tests
        if original is None:
            reg.pop("s3", None)
        else:
            reg["s3"] = original


# ---------------------------------------------------------------------------
# pfio-hdfs (path handling only; no live HDFS)
# ---------------------------------------------------------------------------

def test_hdfs_strip_protocol_drops_netloc():
    assert PfioHdfsFileSystem._strip_protocol(
        "pfio-hdfs://nameservice/a/b") == "/a/b"
    assert PfioHdfsFileSystem._strip_protocol(
        "pfio-hdfs:///a/b") == "/a/b"
    assert PfioHdfsFileSystem._strip_protocol(
        "pfio-hdfs://nameservice/a/b/") == "/a/b"
