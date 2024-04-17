# Test HTTPCachedFS
# TODO: test with hdfs?

import io
import tempfile
import zipfile

from moto import mock_aws
from parameterized import parameterized
from test_fs import gen_fs

from pfio.testing import make_http_server
from pfio.v2 import HTTPCachedFS, from_url


def test_normpath_local():
    with tempfile.TemporaryDirectory() as d:
        with from_url(d) as fs:
            filename = "somefile"
            assert \
                f"file:/{d}/{filename}" == fs._canonical_name(filename)
            zipfilename = "some.zip"
            with fs.open_zip(zipfilename, mode="w") as zipfs:
                assert (
                    f"file:/{d}/{zipfilename}/pfio-zipfs/hoge/fuga" ==
                    zipfs._canonical_name("hoge//fuga")
                )

            foldername = "somefolder"
            with fs.subfs(foldername) as subfs:
                assert (
                    f"file:/{d}/{foldername}/{filename}" ==
                    subfs._canonical_name(filename)
                )


@mock_aws
def test_normpath_s3():
    bucket = "test-dummy-bucket"
    with from_url("s3://{}".format(bucket), create_bucket=True) as fs:
        filename = "somefile"
        assert (
            f"s3://undefined/{bucket}/{filename}" ==
            fs._canonical_name(filename)
        )
        zipfilename = "some.zip"
        with fs.open_zip(zipfilename, mode="w") as zipfs:
            assert (
                f"s3://undefined/{bucket}/{zipfilename}/pfio-zipfs/hoge/fuga"
                ==
                zipfs._canonical_name("hoge//fuga")
            )

        prefixname = "someprefix"
        with fs.subfs(prefixname) as subfs:
            assert (
                f"s3://undefined/{bucket}/{prefixname}/{filename}" ==
                subfs._canonical_name(filename)
            )


@parameterized.expand(["s3", "local"])
@mock_aws
def test_httpcache_simple(target):
    filename = "testfile"
    content = b"deadbeef"

    with make_http_server() as (httpd, port):
        http_cache = f"http://localhost:{port}/"
        cache_content = httpd.RequestHandlerClass.files

        with gen_fs(target) as underlay:
            fs = HTTPCachedFS(http_cache, underlay)

            assert fs.scheme == underlay.scheme
            with fs.open(filename, mode="wb") as fp:
                fp.write(content)
            with fs.open(filename, mode="rb") as fp:
                assert fp.read(-1) == content
            canonical_name = fs._canonical_name(filename)

        assert cache_content["/" + canonical_name] == content


def test_httpcache_too_large():
    from pfio.v2.http_cache import _HTTPCacheIOBase

    filename = "testfile"

    one_mb = 1024 * 1024
    one_mb_array = bytearray(one_mb)

    with make_http_server() as (httpd, port):
        http_cache = f"http://localhost:{port}/"
        cache_content = httpd.RequestHandlerClass.files

        with gen_fs("local") as underlay:
            fs = HTTPCachedFS(http_cache, underlay)
            assert fs.scheme == underlay.scheme

            with fs.open(filename, mode="wb") as fp:
                for _ in range(1024 + 1):  # 1 MB exceeds
                    fp.write(one_mb_array)

            with fs.open(filename, mode="rb") as fp:
                for _ in range(1024 + 1):
                    assert fp.read(one_mb) == one_mb_array
                assert isinstance(fp, _HTTPCacheIOBase)
                assert fp.whole_file is None
                assert fp.tell() == fp.seek(0, io.SEEK_END)

            with fs.open(filename, mode="rb") as fp:
                assert fp.tell() == 0
                fp.seek(one_mb)
                assert fp.tell() == one_mb

                for _ in range(1024):
                    assert fp.read(one_mb) == one_mb_array
                assert isinstance(fp, _HTTPCacheIOBase)
                assert fp.whole_file is None
                assert fp.tell() == fp.seek(0, io.SEEK_END)

        assert len(cache_content) == 0


@parameterized.expand(["s3", "local"])
@mock_aws
def test_httpcache_zipfile_flat(target):
    zipfilename = "test.zip"
    filename1 = "testfile1"
    filecontent1 = b"deadbeef"
    filename2 = "testfile2"
    filecontent2 = b"deadbeeeeeef"

    with make_http_server() as (httpd, port):
        http_cache = f"http://localhost:{port}/"
        cache_content = httpd.RequestHandlerClass.files

        with gen_fs(target) as underlay:
            with underlay.open_zip(zipfilename, mode="w") as zipfs:
                fs = HTTPCachedFS(http_cache, zipfs)
                assert fs.scheme == underlay.scheme

                with fs.open(filename1, mode="wb") as fp:
                    fp.write(filecontent1)
                with fs.open(filename2, mode="wb") as fp:
                    fp.write(filecontent2)

                assert len(cache_content) == 0

            with underlay.open_zip(zipfilename, mode="r") as zipfs:
                fs = HTTPCachedFS(http_cache, zipfs)
                assert fs.scheme == underlay.scheme

                with fs.open(filename1, mode="rb") as fp:
                    assert fp.read(-1) == filecontent1
                with fs.open(filename2, mode="rb") as fp:
                    assert fp.read(-1) == filecontent2

                assert len(cache_content) == 2

        assert (
            cache_content["/" + fs._canonical_name(filename1)] ==
            filecontent1
        )
        assert (
            cache_content["/" + fs._canonical_name(filename2)] ==
            filecontent2
        )


@parameterized.expand(["s3", "local"])
@mock_aws
def test_httpcache_zipfile_archived(target):
    zipfilename = "test.zip"
    filename1 = "testfile1"
    filecontent1 = b"deadbeef"
    filename2 = "testfile2"
    filecontent2 = b"deadbeeeeeef"

    with make_http_server() as (httpd, port):
        http_cache = f"http://localhost:{port}/"
        cache_content = httpd.RequestHandlerClass.files

        with gen_fs(target) as underlay:
            cached_fs = HTTPCachedFS(http_cache, underlay)

            with cached_fs.open_zip(zipfilename, mode="w") as fs:
                assert cached_fs.scheme == underlay.scheme

                with fs.open(filename1, mode="wb") as fp:
                    fp.write(filecontent1)
                with fs.open(filename2, mode="wb") as fp:
                    fp.write(filecontent2)

                assert len(cache_content) == 0

            with cached_fs.open_zip(zipfilename, mode="r") as fs:
                assert cached_fs.scheme == underlay.scheme

                with fs.open(filename1, mode="rb") as fp:
                    assert fp.read(-1) == filecontent1
                with fs.open(filename2, mode="rb") as fp:
                    assert fp.read(-1) == filecontent2

                assert len(cache_content) == 1

        archive_bytes = cache_content["/" +
                                      cached_fs._canonical_name(zipfilename)]
        with io.BytesIO(archive_bytes) as bytesio:
            with zipfile.ZipFile(bytesio) as archive:
                with archive.open(filename1) as fp:
                    assert fp.read(-1) == filecontent1
                with archive.open(filename2) as fp:
                    assert fp.read(-1) == filecontent2
