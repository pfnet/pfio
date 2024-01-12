import os
import pathlib
import tempfile
import uuid
from fnmatch import fnmatch
from sys import version_info as python_version_info
from typing import Any, Iterator, List, Optional, Tuple
from urllib.parse import ParseResult, urlparse, urlunparse

import boto3
import pytest
from moto import mock_aws

from pfio.v2 import Local, from_url
from pfio.v2.pathlib import (Path, PurePath, _has_directory_feature,
                             _removeprefix, unlink)


class TemporaryS3Bucket:
    def __init__(self, name: Optional[str] = None) -> None:
        self._name = name or str(uuid.uuid4())[:8]
        self._client = boto3.resource("s3")

    def __enter__(self) -> str:
        self._client.Bucket(self._name).create()
        return f"s3://{self._name}"

    def __exit__(self, *args: Any) -> None:
        bucket = self._client.Bucket(self._name)
        bucket.objects.delete()
        bucket.delete()


@pytest.fixture(params=["local", "s3"])
def temporary_storage(request: pytest.FixtureRequest) -> Iterator[str]:
    if request.param == "local":
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    elif request.param == "s3":
        with mock_aws():
            with TemporaryS3Bucket() as tmpbucket:
                yield tmpbucket
    else:
        raise RuntimeError


@pytest.fixture
def storage(temporary_storage: str) -> Iterator[str]:
    with from_url(temporary_storage) as fs:
        fs.makedirs("my/library/zoo")
        fs.makedirs("my/foo/bar")

        with fs.open("my/library/zoo/abc.txt", mode="wb") as f:
            f.write(b"abcXYZ")

        with fs.open("my/foo/bar/README.md", mode="w") as f:
            f.write("HOWTO")

        with fs.open("my/library/setup.py", mode="w") as f:
            f.write("import os")

        with fs.open("my/library/hoge.txt", mode="w") as f:
            f.write("")

        with fs.open("my/foo/zoo", mode="wb") as f:
            f.write(b"hello")

        with fs.open("my/library.tar.gz", mode="wb") as f:
            f.write(b"body of library.tar.gz")

    yield temporary_storage


@pytest.mark.parametrize(
    "p",
    [
        (),
        ("",),
        (".",),
        ("/",),
        ("my/library",),
        ("my/library/setup.py",),
        ("/usr/local/python",),
    ],
)
def test_purepath_parts(
    temporary_storage: str,
    p: Tuple[str, ...],
) -> None:
    with from_url(temporary_storage) as fs:
        actual = PurePath(*p, fs=fs)
        expected = pathlib.PurePosixPath(*p)
        assert actual.parts == expected.parts


@pytest.mark.parametrize(
    "p",
    [
        (),
        ("",),
        (".",),
        ("/",),
        ("my/library",),
        ("my/library/setup.py",),
        ("/usr/local/python",),
    ],
)
def test_purepath_string_operators(
    temporary_storage: str,
    p: Tuple[str, ...],
) -> None:
    with from_url(temporary_storage) as fs:
        actual = PurePath(*p, fs=fs)
        expected = pathlib.PurePosixPath(*p)
        assert os.fspath(actual) == os.fspath(expected)
        assert str(actual) == str(expected)
        assert repr(actual) == repr(expected).replace(
            "PurePosixPath", "PurePath"
        )
        assert bytes(actual) == bytes(expected)


@pytest.mark.parametrize(
    "paths",
    [
        [(), ("/",)],
        [("",), ("/usr",)],
        [(".",), ("/tmp",)],
        [("/",), (".",)],
        [("my/library",), ("/my/library",)],
        [("my/library/setup.py",), ("/my/library/setup.py",)],
        [("/usr/local/python",), ("/usr/local/python3",)],
    ],
)
def test_purepath_equal_operator(
    temporary_storage: str,
    paths: List[Tuple[str, ...]],
) -> None:
    with from_url(temporary_storage) as fs:
        p, q = paths
        expected = pathlib.PurePosixPath(*p)
        actual = PurePath(*p, fs=fs)
        assert expected == pathlib.PurePosixPath(*p)
        assert expected != pathlib.PurePosixPath(*q)
        assert actual == PurePath(*p, fs=fs)
        assert actual != PurePath(*q, fs=fs)


def test_purepath_truediv_operator(temporary_storage: str) -> None:
    with from_url(temporary_storage) as fs:
        expected = pathlib.PurePosixPath("/usr")
        actual = PurePath("/usr", fs=fs)
        assert expected / "hoge" == pathlib.PurePosixPath("/usr/hoge")
        assert actual / "hoge" == PurePath("/usr/hoge", fs=fs)
        assert expected / pathlib.PurePosixPath(
            "hoge"
        ) == pathlib.PurePosixPath("/usr/hoge")
        assert actual / PurePath("hoge", fs=fs) == PurePath("/usr/hoge", fs=fs)

        expected = pathlib.PurePosixPath("usr")
        actual = PurePath("usr", fs=fs)
        assert expected / "hoge" == pathlib.PurePosixPath("usr/hoge")
        assert actual / "hoge" == PurePath("usr/hoge", fs=fs)
        assert expected / pathlib.PurePosixPath(
            "hoge"
        ) == pathlib.PurePosixPath("usr/hoge")
        assert actual / PurePath("hoge", fs=fs) == PurePath("usr/hoge", fs=fs)

        expected = pathlib.PurePosixPath("/etc")
        actual = PurePath("/etc", fs=fs)
        assert expected / "/usr" == pathlib.PurePosixPath("/usr")
        assert actual / "/usr" == PurePath("/usr", fs=fs)
        assert expected / pathlib.PurePosixPath(
            "/usr"
        ) == pathlib.PurePosixPath("/usr")
        assert actual / PurePath("/usr", fs=fs) == PurePath("/usr", fs=fs)

        expected = pathlib.PurePosixPath("etc")
        actual = PurePath("etc", fs=fs)
        assert expected / "/usr" == pathlib.PurePosixPath("/usr")
        assert actual / "/usr" == PurePath("/usr", fs=fs)
        assert expected / pathlib.PurePosixPath(
            "/usr"
        ) == pathlib.PurePosixPath("/usr")
        assert actual / PurePath("/usr", fs=fs) == PurePath("/usr", fs=fs)


def test_purepath_rtruediv_operator(temporary_storage: str) -> None:
    with from_url(temporary_storage) as fs:
        expected = pathlib.PurePosixPath("usr")
        actual = PurePath("usr", fs=fs)
        assert "/etc" / expected == pathlib.PurePosixPath("/etc/usr")
        assert "/etc" / actual == PurePath("/etc/usr", fs=fs)
        assert pathlib.PurePosixPath(
            "/etc"
        ) / expected == pathlib.PurePosixPath("/etc/usr")
        assert PurePath("/etc", fs=fs) / actual == PurePath("/etc/usr", fs=fs)

        expected = pathlib.PurePosixPath("usr")
        actual = PurePath("usr", fs=fs)
        assert "etc" / expected == pathlib.PurePosixPath("etc/usr")
        assert "etc" / actual == PurePath("etc/usr", fs=fs)
        assert pathlib.PurePosixPath(
            "etc"
        ) / expected == pathlib.PurePosixPath("etc/usr")
        assert PurePath("etc", fs=fs) / actual == PurePath("etc/usr", fs=fs)

        expected = pathlib.PurePosixPath("/etc")
        actual = PurePath("/etc", fs=fs)
        assert "/usr" / expected == pathlib.PurePosixPath("/etc")
        assert "/usr" / actual == PurePath("/etc", fs=fs)
        assert pathlib.PurePosixPath(
            "/usr"
        ) / expected == pathlib.PurePosixPath("/etc")
        assert PurePath("/usr", fs=fs) / actual == PurePath("/etc", fs=fs)

        expected = pathlib.PurePosixPath("/etc")
        actual = PurePath("/etc", fs=fs)
        assert "usr" / expected == pathlib.PurePosixPath("/etc")
        assert "usr" / actual == PurePath("/etc", fs=fs)
        assert pathlib.PurePosixPath(
            "usr"
        ) / expected == pathlib.PurePosixPath("/etc")
        assert PurePath("usr", fs=fs) / actual == PurePath("/etc", fs=fs)


def test_purepath_joinpath(temporary_storage: str) -> None:
    with from_url(temporary_storage) as fs:
        expected = pathlib.PurePosixPath("/usr")
        actual = PurePath("/usr", fs=fs)
        assert expected.joinpath("hoge") == pathlib.PurePosixPath("/usr/hoge")
        assert actual.joinpath("hoge") == PurePath("/usr/hoge", fs=fs)
        assert expected.joinpath(
            pathlib.PurePosixPath("hoge")
        ) == pathlib.PurePosixPath("/usr/hoge")
        assert actual.joinpath(PurePath("hoge", fs=fs)) == PurePath(
            "/usr/hoge", fs=fs
        )

        expected = pathlib.PurePosixPath("usr")
        actual = PurePath("usr", fs=fs)
        assert expected.joinpath("hoge") == pathlib.PurePosixPath("usr/hoge")
        assert actual.joinpath("hoge") == PurePath("usr/hoge", fs=fs)
        assert expected.joinpath(
            pathlib.PurePosixPath("hoge")
        ) == pathlib.PurePosixPath("usr/hoge")
        assert actual.joinpath(PurePath("hoge", fs=fs)) == PurePath(
            "usr/hoge", fs=fs
        )

        expected = pathlib.PurePosixPath("/etc")
        actual = PurePath("/etc", fs=fs)
        assert expected.joinpath("/usr") == pathlib.PurePosixPath("/usr")
        assert actual.joinpath("/usr") == PurePath("/usr", fs=fs)
        assert expected.joinpath(
            pathlib.PurePosixPath("/usr")
        ) == pathlib.PurePosixPath("/usr")
        assert actual.joinpath(PurePath("/usr", fs=fs)) == PurePath(
            "/usr", fs=fs
        )

        expected = pathlib.PurePosixPath("etc")
        actual = PurePath("etc", fs=fs)
        assert expected.joinpath("/usr") == pathlib.PurePosixPath("/usr")
        assert actual.joinpath("/usr") == PurePath("/usr", fs=fs)
        assert expected.joinpath(
            pathlib.PurePosixPath("/usr")
        ) == pathlib.PurePosixPath("/usr")
        assert actual.joinpath(PurePath("/usr", fs=fs)) == PurePath(
            "/usr", fs=fs
        )


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/",
        "/usr",
        "my/library/setup.py",
    ],
)
def test_purepath_parents(
    temporary_storage: str,
    path: str,
) -> None:
    with from_url(temporary_storage) as fs:
        expected = pathlib.PurePosixPath(path)
        actual = PurePath(path, fs=fs)

        expected_parents = expected.parents
        actual_parents = actual.parents
        assert len(actual_parents) == len(expected_parents)

        for p, q in zip(actual_parents, expected_parents):
            print(p.parts, q.parts)
            assert p.parts == q.parts

        assert actual.parent.parts == expected.parent.parts


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/",
        ".",
        "/usr/bin/python3",
        "my/library",
        "/my/library/setup.py",
        "my/library.tar.gz",
    ],
)
def test_purepath_compatible_properties(
    temporary_storage: str,
    path: str,
) -> None:
    with from_url(temporary_storage) as fs:
        expected = pathlib.PurePosixPath(path)
        actual = PurePath(path, fs=fs)

        assert actual.drive == expected.drive
        assert actual.root == expected.root
        assert actual.anchor == expected.anchor
        assert actual.name == expected.name
        assert actual.suffix == expected.suffix
        assert actual.suffixes == expected.suffixes
        assert actual.stem == expected.stem


def test_purepath_relative_to(temporary_storage: str) -> None:
    with from_url(temporary_storage) as fs:
        excepted = pathlib.PurePosixPath("/etc/passwd")
        actual = PurePath("/etc/passwd", fs=fs)

        assert excepted.relative_to("/") == pathlib.PurePosixPath("etc/passwd")
        assert actual.relative_to("/") == PurePath("etc/passwd", fs=fs)

        assert excepted.relative_to("/", "etc") == pathlib.PurePosixPath(
            "passwd"
        )
        assert actual.relative_to("/", "etc") == PurePath("passwd", fs=fs)

        excepted = pathlib.PurePosixPath("usr/local/lib")
        actual = PurePath("usr/local/lib", fs=fs)

        assert excepted.relative_to("usr/local") == pathlib.PurePosixPath(
            "lib"
        )
        assert actual.relative_to("usr/local") == PurePath("lib", fs=fs)

        with pytest.raises(ValueError):
            excepted.relative_to("/usr")

        with pytest.raises(ValueError):
            actual.relative_to("/usr")

        excepted = pathlib.PurePosixPath("usr/local/lib/")
        actual = PurePath("usr/local/lib/", fs=fs)

        assert excepted.relative_to("usr/local/") == pathlib.PurePosixPath(
            "lib/"
        )
        assert actual.relative_to("usr/local/") == PurePath("lib/", fs=fs)


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/",
        ".",
        "/usr/bin/python3",
        "my/library",
        "/my/library/setup.py",
        "my/library.tar.gz",
    ],
)
def test_purepath_compatible_methods(
    temporary_storage: str,
    path: str,
) -> None:
    with from_url(temporary_storage) as fs:
        expected = pathlib.PurePath(path)
        actual = PurePath(path, fs=fs)

        assert actual.as_posix() == expected.as_posix()
        assert actual.is_absolute() == expected.is_absolute()
        assert actual.is_reserved() == expected.is_reserved()

        if python_version_info.minor > 8:
            assert expected.is_relative_to(str(actual))
            assert actual.is_relative_to(str(expected))
            assert not expected.is_relative_to("/mnt")
            assert not actual.is_relative_to("/mnt")

        if actual.is_absolute():
            parsed = urlparse(temporary_storage, scheme="file")
            unparsed = ParseResult(
                parsed.scheme,
                parsed.netloc,
                os.path.join(parsed.path, _removeprefix(path, "/")),
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
            assert actual.as_uri() == urlunparse(unparsed)
        else:
            with pytest.raises(ValueError):
                expected.as_uri()

            with pytest.raises(ValueError):
                actual.as_uri()

        if expected.name:
            assert str(actual.with_name("XYZ.abc")) == str(
                expected.with_name("XYZ.abc")
            )
        else:
            with pytest.raises(ValueError):
                actual.with_name("XYZ.abc")

        if python_version_info.minor > 8:
            if expected.stem:
                assert str(actual.with_stem("a2z")) == str(
                    expected.with_stem("a2z")
                )
            else:
                with pytest.raises(ValueError):
                    actual.with_stem("a2z")

        if expected.name:
            assert str(actual.with_suffix(".bz2")) == str(
                expected.with_suffix(".bz2")
            )
        else:
            with pytest.raises(ValueError):
                actual.with_suffix(".bz2")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/",
        ".",
        "/usr/bin/python3",
        "my/library",
        "/my/library/setup.py",
        "my/library.tar.gz",
    ],
)
def test_path_stat(storage: str, path: str) -> None:
    with from_url(url=storage) as fs:
        p = Path(_removeprefix(path, "/"), fs=fs)
        if p.is_file():
            stat = p.stat()
            assert stat.size > 0 or stat.last_modified is not None


def test_absolute_resolve(storage: str) -> None:
    with from_url(url=storage) as fs:
        p = Path("my/library/../setup.py", fs=fs)

        assert p.absolute() == Path("/my/library/../setup.py", fs=fs)
        assert p.resolve() == Path("/my/setup.py", fs=fs)

        with pytest.raises(FileNotFoundError):
            p.resolve(strict=True)

        resolved = Path("my/library/dummy/../setup.py", fs=fs).resolve(
            strict=True
        )
        assert resolved == Path("/my/library/setup.py", fs=fs)

        if isinstance(fs, Local):
            expected = pathlib.PosixPath(storage, "my/library/../setup.py")
            assert expected.absolute() == pathlib.PosixPath(
                storage, "my/library/../setup.py"
            )
            assert expected.resolve() == pathlib.PosixPath(
                storage, "my/setup.py"
            )
            with pytest.raises(FileNotFoundError):
                expected.resolve(strict=True)


def test_path_file(storage: str) -> None:
    with from_url(url=storage) as fs:
        p = Path("/my/hoge.txt", fs=fs)
        p.touch(exist_ok=False)
        p.touch(exist_ok=True)
        assert p.exists()
        assert p.is_file()

        p.unlink(missing_ok=False)
        p.unlink(missing_ok=True)
        assert not p.exists()

        assert p.write_bytes(b"hello binary world") == len(
            b"hello binary world"
        )
        assert p.read_bytes() == b"hello binary world"

        assert p.write_text("hello world") == len("hello world")
        assert p.read_text() == "hello world"

        q = Path("/my/library/hoge.txt", fs=fs)
        old_data = q.read_bytes()
        p.rename(q)
        assert not p.exists()
        assert q.read_bytes() != old_data

        p = Path("/my/library.tar.gz", fs=fs)
        old_data = p.read_bytes()
        q.replace(p)
        assert not q.exists()
        assert p.read_bytes() != old_data


def test_path_directory(storage: str) -> None:
    with from_url(url=storage) as fs:
        p = Path("my/local", fs=fs)
        assert not p.exists()
        assert not p.is_dir()

        p.mkdir(parents=False, exist_ok=False)
        p.mkdir(parents=False, exist_ok=True)

        # FIXME: `S3.makedirs` not creates zero-length object.
        dummy = p / "dummy"
        dummy.write_bytes(b"dummy data")
        assert p.exists()
        assert p.is_dir()

        dummy.unlink()
        p.rmdir()
        assert not p.is_dir()

        if _has_directory_feature(p._fs):
            with pytest.raises(NotADirectoryError):
                p.rmdir()

            with pytest.raises(NotADirectoryError):
                Path("my/library/hoge.txt", fs=fs).rmdir()

        p = Path("my/library", fs=fs)

        q = p.rename("my/local")
        assert str(q) == "my/local"
        assert (q / "setup.py").exists()

        assert q.exists()
        assert not p.exists()
        assert q.is_dir()
        assert not p.is_dir()

        p = Path("my/local", fs=fs)
        q = p.replace("my/library")
        assert str(q) == "my/library"
        assert (q / "setup.py").exists()

        assert q.exists()
        assert not p.exists()
        assert q.is_dir()
        assert not p.is_dir()


@pytest.mark.parametrize("pattern", ["my/**/*", "my/**/*.py"])
def test_path_glob(storage: str, pattern: str) -> None:
    with from_url(url=storage) as fs:
        actual = Path(fs=fs)

        expected_entries = [
            "my",
            "my/library",
            "my/library/zoo",
            "my/foo",
            "my/foo/bar",
            "my/library/zoo/abc.txt",
            "my/foo/bar/README.md",
            "my/library/setup.py",
            "my/library/hoge.txt",
            "my/foo/zoo",
            "my/library.tar.gz",
        ]
        _pattern = pattern.replace("**/*", "*")
        expected_entries = list(
            filter(lambda x: fnmatch(x, _pattern), expected_entries)
        )
        actual_entries = list(map(str, actual.glob(pattern)))
        assert sorted(actual_entries) == sorted(expected_entries)


@pytest.mark.parametrize("pattern", ["*", "*.py"])
def test_path_rglob(storage: str, pattern: str) -> None:
    with from_url(url=storage) as fs:
        actual = Path(fs=fs)

        expected_entries = [
            "my",
            "my/library",
            "my/library/zoo",
            "my/foo",
            "my/foo/bar",
            "my/library/zoo/abc.txt",
            "my/foo/bar/README.md",
            "my/library/setup.py",
            "my/library/hoge.txt",
            "my/foo/zoo",
            "my/library.tar.gz",
        ]
        expected_entries = list(
            filter(lambda x: fnmatch(x, pattern), expected_entries)
        )
        actual_entries = list(map(str, actual.rglob(pattern)))
        assert sorted(actual_entries) == sorted(expected_entries)


@pytest.mark.parametrize("path", ["my", "my/library", "."])
def test_path_iterdir(storage: str, path: str) -> None:
    with from_url(url=storage) as fs:
        actual = Path(path, fs=fs)

        expected_entries = [
            "my",
            "my/library",
            "my/library/zoo",
            "my/foo",
            "my/foo/bar",
            "my/library/zoo/abc.txt",
            "my/foo/bar/README.md",
            "my/library/setup.py",
            "my/library/hoge.txt",
            "my/foo/zoo",
            "my/library.tar.gz",
        ]

        filtered: list[str] = []
        for item in expected_entries:
            if (
                pathlib.PurePosixPath(item).parent.parts
                == pathlib.PurePosixPath(path).parts
            ):
                filtered.append(item)

        actual_entries = list(map(str, actual.iterdir()))
        assert sorted(actual_entries) == sorted(filtered)


def test_unlink(storage: str) -> None:
    with from_url(url=storage) as fs:
        target = Path("my", fs=fs)

        assert len(list(target.iterdir())) > 0

        unlink(target)
        assert not target.exists()
        assert not target.is_dir()

        with pytest.raises(NotADirectoryError):
            next(target.iterdir())

        for entry in target.rglob("*"):
            print(entry)

        assert len(list(target.rglob("*"))) == 0
