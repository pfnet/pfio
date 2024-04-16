import functools
import uuid
from fnmatch import fnmatch, fnmatchcase
from io import IOBase
from os import PathLike
from pathlib import PurePosixPath
from posixpath import join as joinpath
from posixpath import normpath
from sys import version_info as python_version_info
from typing import (Callable, Iterator, List, Optional, Sequence, Set, Tuple,
                    Type, TypeVar, Union)
from urllib.parse import ParseResult, urlunparse

from .fs import FS, FileStat
from .hdfs import Hdfs
from .local import Local
from .s3 import S3
from .zip import Zip

SelfPurePathType = TypeVar("SelfPurePathType", bound="PurePath")
SelfPathType = TypeVar("SelfPathType", bound="Path")


@functools.lru_cache(maxsize=16)
def _has_directory_feature(fs: FS) -> bool:
    if isinstance(fs, S3):
        # NOTE: Apache Ozone supports supports directories
        #       depending on its configuration.
        #       refers to `ozone.om.enable.filesystem.paths` property.
        dpath = f"__pfio_check_{str(uuid.uuid4())[:8]}"
        upath = f"{dpath}/__tmp"

        with fs.open(upath, mode="wb") as f:
            f.write(b"")
        fs.remove(upath)

        # NOTE: directory creates automatically
        #       if `ozone.om.enable.filesystem.paths` is enabled.
        if fs.exists(dpath):
            fs.remove(dpath)
            return True
        else:
            return False
    else:
        return True


def _removeprefix(text: str, prefix: str) -> str:
    # NOTE: `str.removeprefix` supports version 3.9 or higher.
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def _compare_fs(lhs: FS, rhs: FS) -> bool:
    if type(lhs) is type(rhs):
        assert isinstance(lhs.cwd, str)
        assert isinstance(rhs.cwd, str)

        if isinstance(lhs, Local) and isinstance(rhs, Local):
            return lhs.cwd == rhs.cwd
        elif isinstance(lhs, S3) and isinstance(rhs, S3):
            return (
                lhs.cwd == rhs.cwd
                and lhs.bucket == rhs.bucket
                and lhs.endpoint == rhs.endpoint
            )
        elif isinstance(lhs, Hdfs) and isinstance(rhs, Hdfs):
            return lhs.cwd == rhs.cwd and lhs.username == rhs.username
        elif isinstance(lhs, Zip) and isinstance(rhs, Zip):
            return lhs.cwd == rhs.cwd
        else:
            raise ValueError(f"unsupported FS: {lhs} and {rhs}")

    return False


def _not_supported(name: Optional[str] = None) -> NotImplementedError:
    if not name:
        import inspect

        stack = inspect.stack()
        if "self" in stack[1].frame.f_locals:
            cname = stack[1].frame.f_locals["self"].__class__.__name__
        elif "cls" in stack[1].frame.f_locals:
            cname = stack[1].frame.f_locals["cls"].__name__
        else:
            cname = ""

        fname = stack[1].function
        name = f"{cname}.{fname}" if cname else fname

    return NotImplementedError(f"`{name}` is unsupported on this system")


class PurePath(PathLike):
    """
    `pathlib.PurePosixPath` compatible interface.

    Args:
        args: construct paths.
        fs: target file system.
        scheme: specify URL scheme. (for `as_uri` method)

    Note:
        It conforms to `pathlib.PurePosixPath` of Python 3.12 specification.

        this class not inherits any `pathlib` classes because
        pfio filesystems is not suitable for pathlib abstact
        and helper classes.

    TODO:
        `scheme` should moves to `FS`.
    """

    def __init__(
        self,
        *args: Union[str, PathLike],
        fs: FS,
        scheme: Optional[str] = None,
    ) -> None:
        if isinstance(fs, Local):
            scheme = scheme or "file"
        elif isinstance(fs, S3):
            scheme = scheme or "s3"
        elif isinstance(fs, Hdfs):
            scheme = scheme or "hdfs"
        elif isinstance(fs, Zip):
            scheme = scheme or ""
        else:
            raise ValueError(f"unsupported FS: {fs}")

        self._fs: FS = fs
        self._scheme = scheme
        self._pure = PurePosixPath(*args)
        self._hash = hash(self._pure) + hash(self._fs) + hash(self._scheme)

    @property
    def sep(self) -> str:
        return "/"

    @property
    def scheme(self) -> str:
        return self._scheme

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PurePath):
            return self._pure == other._pure and _compare_fs(
                self._fs, other._fs
            )
        else:
            return NotImplemented

    def __fspath__(self) -> str:
        return self._pure.__fspath__()

    def __str__(self) -> str:
        return self.__fspath__()

    def __repr__(self) -> str:
        return f"{type(self).__name__}('{self.__fspath__()}')"

    def __bytes__(self) -> bytes:
        return self._pure.__bytes__()

    def __truediv__(
        self: SelfPurePathType,
        other: Union[str, PathLike, SelfPurePathType],
    ) -> SelfPurePathType:
        try:
            return self.with_segments(self._pure / str(other))
        except TypeError:
            return NotImplemented

    def __rtruediv__(
        self: SelfPurePathType,
        other: Union[str, PathLike, SelfPurePathType],
    ) -> SelfPurePathType:
        try:
            return self.with_segments(str(other) / self._pure)
        except TypeError:
            return NotImplemented

    # ---------------------------------------
    # pathlib.PurePath compatible properties
    # ---------------------------------------

    @property
    def parts(self) -> Tuple[str, ...]:
        return self._pure.parts

    @property
    def drive(self) -> str:
        return self._pure.drive

    @property
    def root(self) -> str:
        return self._pure.root

    @property
    def anchor(self) -> str:
        return self._pure.anchor

    @property
    def parents(self: SelfPurePathType) -> Sequence[SelfPurePathType]:
        # FIXME: reduce costs
        p = self.parts
        paths = [self.with_segments(*p[:-i]) for i in range(1, len(p) + 1)]
        if self.is_absolute():
            paths.pop(-1)
        return paths

    @property
    def parent(self: SelfPurePathType) -> SelfPurePathType:
        return self.with_segments(self._pure.parent)

    @property
    def name(self) -> str:
        return self._pure.name

    @property
    def suffix(self) -> str:
        return self._pure.suffix

    @property
    def suffixes(self) -> List[str]:
        return self._pure.suffixes

    @property
    def stem(self) -> str:
        return self._pure.stem

    # ---------------------------------------
    # pathlib.PurePath compatible methods
    # ---------------------------------------

    def as_posix(self) -> str:
        return self._pure.as_posix()

    def as_uri(self) -> str:
        if self.is_absolute():
            if isinstance(self._fs, S3):
                netloc = self._fs.bucket
                assert isinstance(netloc, str)
            elif isinstance(self._fs, Hdfs):
                raise NotImplementedError
            else:
                netloc = ""

            prefix = self._fs.cwd
            assert isinstance(prefix, str)
            key = _removeprefix(self.as_posix(), self.sep)

            parsed = ParseResult(
                scheme=self.scheme,
                netloc=netloc,
                path=joinpath(prefix, key),
                params="",
                query="",
                fragment="",
            )
            return urlunparse(parsed)

        else:
            raise ValueError(f"'{self}' is not absolute path")

    def is_absolute(self) -> bool:
        return self._pure.is_absolute()

    def is_relative_to(self, *other: Union[str, PathLike]) -> bool:
        if python_version_info.minor < 9:
            raise NotImplementedError(
                "`is_relative_to()` supports python 3.9 or higher"
            )
        else:
            return self._pure.is_relative_to(*other)  # type: ignore

    def is_reserved(self) -> bool:
        return self._pure.is_reserved()

    def joinpath(
        self: SelfPurePathType,
        *args: Union[str, PathLike],
    ) -> SelfPurePathType:
        return self.with_segments(self._pure.joinpath(*args))

    def match(
        self,
        pattern: Union[str, PathLike],
        *,
        case_sensitive: bool = False,
    ) -> bool:
        pattern = str(pattern)
        if case_sensitive:
            return fnmatchcase(self.as_uri(), pattern)
        else:
            return fnmatch(self.as_uri(), pattern)

    def relative_to(
        self: SelfPurePathType,
        *other: Union[str, PathLike],
        walk_up: bool = False,
    ) -> SelfPurePathType:
        if python_version_info.minor < 12 and walk_up:
            raise NotImplementedError(
                "`walk_up=True` supports python version 3.12 or higher"
            )

        if python_version_info.minor >= 12:
            rel = self._pure.relative_to(
                *other,
                walk_up=walk_up,  # type: ignore
            )
        else:
            rel = self._pure.relative_to(*other)

        return self.with_segments(rel)

    def with_name(self: SelfPurePathType, name: str) -> SelfPurePathType:
        return self.with_segments(self._pure.with_name(name))

    def with_stem(self: SelfPurePathType, stem: str) -> SelfPurePathType:
        if python_version_info.minor < 9:
            raise NotImplementedError(
                "`with_stem()` supports python 3.9 or higher"
            )
        else:
            return self.with_segments(
                self._pure.with_stem(stem)  # type: ignore
            )

    def with_suffix(self: SelfPurePathType, suffix: str) -> SelfPurePathType:
        return self.with_segments(self._pure.with_suffix(suffix))

    def with_segments(
        self: SelfPurePathType,
        *args: Union[str, PathLike],
    ) -> SelfPurePathType:
        return type(self)(*args, fs=self._fs, scheme=self.scheme)


class Path(PurePath):
    """
    `pathlib.PosixPath` compatible interface.

    Args:
        args: construct paths.
        fs: target file system.
        scheme: specify URL scheme. (for `as_uri` method)

    Note:
        many methods raise `NotImplementedError`
        because they require unsupported features of `FS`.

        several methods behave slightly different,
        - `stat` returns `FileStat` object instead of `os.stat_result`.
        - `glob`, `rglob`, `iterdir` will not return directory type object.
    """

    def __init__(
        self,
        *args: str,
        fs: FS,
        scheme: Optional[str] = None,
    ) -> None:
        super().__init__(*args, fs=fs, scheme=scheme)

    def _as_relative_to_fs(self) -> str:
        return _removeprefix(self.as_posix(), self.anchor)

    # ---------------------------------------
    # pathlib.Path compatible classmethods
    # ---------------------------------------

    @classmethod
    def cwd(cls: Type[SelfPathType]) -> SelfPathType:
        raise _not_supported()

    @classmethod
    def home(cls: Type[SelfPathType]) -> SelfPathType:
        raise _not_supported()

    # ---------------------------------------
    # pathlib.Path compatible methods
    # ---------------------------------------

    def stat(self, *, follow_symlinks: bool = True) -> FileStat:
        if not follow_symlinks:
            raise _not_supported("follow_symlinks=False")

        return self._fs.stat(self._as_relative_to_fs())

    def chmod(self, *, mode: int, follow_symlinks: bool = True) -> None:
        raise _not_supported()

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        if not follow_symlinks:
            raise _not_supported("follow_symlinks=False")

        return self.is_file() or self.is_dir()

    def expanduser(self: SelfPathType) -> SelfPathType:
        raise _not_supported()

    def glob(
        self: SelfPathType,
        pattern: str,
        *,
        case_sensitive: Optional[bool] = None,
    ) -> Iterator[SelfPathType]:
        if case_sensitive is not None:
            raise _not_supported("case_sensitive=True or False")

        match_ = fnmatchcase if case_sensitive else fnmatch

        p = self._as_relative_to_fs()
        recursive = "**" in pattern

        # NOTE: `pathlib.Path.glob` interprets "**/*" to "*"
        if pattern.endswith("**/*"):
            pattern = pattern.replace("**/*", "*")
        pattern = pattern.replace("**", "*")

        prefixes: Set[SelfPathType] = set()

        # NOTE: `S3.glob` is not implemented...
        #       it use `FS.list` instead of `glob`.
        try:
            for entry in self._fs.list(p, recursive=recursive):
                assert isinstance(entry, str)

                e = self.with_segments(*self.parts)
                for part in entry.split(self.sep):
                    if part:
                        e = e / part
                        if e not in prefixes and match_(str(e), pattern):
                            prefixes.add(e)
                            yield e
        except FileNotFoundError:
            # NOTE: raise from `os.scandir` in `Local` class.
            pass

    def group(self) -> str:
        raise _not_supported()

    def is_dir(self) -> bool:
        p = self._as_relative_to_fs() + self.sep

        if _has_directory_feature(self._fs):
            return self._fs.isdir(p)  # type: ignore
        else:
            # FIXME: `S3.isdir` has a bug (?)
            for entry in self._fs.list(p, recursive=True):
                assert isinstance(entry, str)
                if entry and not entry.endswith(self.sep):
                    return True
            return False

    def is_file(self) -> bool:
        p = self._as_relative_to_fs()
        return self._fs.exists(p) and not self._fs.isdir(p)

    def is_junction(self) -> bool:
        return False  # only Windows supports junctions

    def is_mount(self) -> bool:
        raise _not_supported()

    def is_symlink(self) -> bool:
        raise _not_supported()

    def is_socket(self) -> bool:
        raise _not_supported()

    def is_fifo(self) -> bool:
        raise _not_supported()

    def is_block_device(self) -> bool:
        raise _not_supported()

    def is_char_device(self) -> bool:
        raise _not_supported()

    def iterdir(self: SelfPathType) -> Iterator[SelfPathType]:
        if self.is_dir():
            for entry in self._fs.list(self._as_relative_to_fs()):
                assert isinstance(entry, str)
                yield self.with_segments(*self.parts, entry)
        else:
            raise NotADirectoryError(f"'{self.as_posix()}' is not a directory")

    def walk(
        self,
        top_down: bool = True,
        on_error: Optional[Callable] = None,
        follow_symlinks: bool = False,
    ) -> Iterator[Tuple["Path", List[str], List[str]]]:
        raise _not_supported()

    def lchmod(self, mode: int) -> None:
        raise _not_supported()

    def lstat(self) -> FileStat:
        raise _not_supported()

    def mkdir(
        self,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        if _has_directory_feature(self._fs):
            if not parents and not self.parent.is_dir():
                raise FileNotFoundError(
                    f"'{self.parent.as_posix()}' is not a directory"
                )

            self._fs.makedirs(
                self._as_relative_to_fs(),
                mode=mode,
                exist_ok=exist_ok,
            )

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> IOBase:
        # NOTE: first argument is `file_path`` in `Local`,
        #       but `S3` is `path`.
        return self._fs.open(  # type: ignore
            self._as_relative_to_fs(),
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def owner(self) -> str:
        raise _not_supported()

    def read_bytes(self) -> bytes:
        with self.open(mode="rb", buffering=0) as f:
            return f.read()  # type: ignore

    def read_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> str:
        with self.open(mode="rt", encoding=encoding, errors=errors) as f:
            return f.read()  # type: ignore

    def readlink(self: SelfPathType) -> SelfPathType:
        raise _not_supported()

    def rename(
        self: SelfPathType,
        target: Union[str, PurePath],
    ) -> SelfPathType:
        if isinstance(self._fs, S3):
            # NOTE: S3 API does not support rename a `prefix`.
            #       we implemented by `copy` + `remove`.
            #       it is generic but poor performance...
            target = self.with_segments(
                _removeprefix(str(target), self.anchor)
            )
            copy(self, target)
            unlink(self)
        else:
            target = (
                _removeprefix(target.as_posix(), self.anchor)
                if isinstance(target, PurePath)
                else target
            )
            self._fs.rename(self._as_relative_to_fs(), target)

        return self.with_segments(str(target))

    def replace(
        self: SelfPathType,
        target: Union[str, PurePath],
    ) -> SelfPathType:
        return self.rename(target)

    def absolute(self: SelfPathType) -> SelfPathType:
        return self.with_segments("/" / self._pure)

    def resolve(self: SelfPathType, strict: bool = False) -> SelfPathType:
        # NOTE: symbolic link is not supported
        normalized_path = normpath(self._as_relative_to_fs())
        resolved_path = self.with_segments(normalized_path).absolute()
        if strict and not resolved_path.exists():
            raise FileNotFoundError(f"'{self.as_posix()}' not found")
        return resolved_path

    def rglob(
        self: SelfPathType,
        pattern: str,
        *,
        case_sensitive: Optional[bool] = None,
    ) -> Iterator[SelfPathType]:
        return self.glob(
            joinpath("**", _removeprefix(pattern, self.sep)),
            case_sensitive=case_sensitive,
        )

    def rmdir(self) -> None:
        if _has_directory_feature(self._fs):
            if self.is_dir():
                self._fs.remove(self._as_relative_to_fs())
            else:
                raise NotADirectoryError(
                    f"'{self.as_posix()}' is not a directory"
                )

    def samefile(self, other_path: Union[str, SelfPathType]) -> bool:
        raise _not_supported()

    def symlink_to(
        self,
        target: Union[str, PathLike],
        target_is_directory: bool = False,
    ) -> None:
        raise _not_supported()

    def hardlink_to(self, target: Union[str, PathLike]) -> None:
        raise _not_supported()

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        if self.exists() and not exist_ok:
            raise FileExistsError(f"'{self.as_posix()}' exists")

        with self.open("wb") as f:
            f.write(b"")

    def unlink(self, missing_ok: bool = False) -> None:
        if self.is_dir():
            raise IsADirectoryError(f"'{self.as_posix()}' is a directory")
        elif self.is_file():
            self._fs.remove(self._as_relative_to_fs())
        elif not missing_ok:
            raise FileNotFoundError(f"'{self.as_posix()}' is not a file")

    def write_bytes(self, data: bytes) -> int:
        with self.open(mode="wb") as f:
            return f.write(data)  # type: ignore

    def write_text(
        self,
        data: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> int:
        with self.open(
            mode="wt",
            encoding=encoding,
            errors=errors,
            newline=newline,
        ) as f:
            return f.write(data)  # type: ignore


def copy(
    src: Path,
    dst: Path,
    *,
    chunk_size: int = 16 * (2**20),
) -> None:
    """
    simplified copy interface.

    Args:
        src: source path of file or directory.
        dst: destination path of file or directory.
        chunk_size: blob size to copy object.

    Note:
        if `src` is a directory, all child entries in
        directory will copied to `dst` recursively.
    """

    src = src.resolve()
    dst = dst.resolve()

    if src.is_file():
        if dst.is_dir():
            raise IsADirectoryError(f"{dst.as_uri()} is a directory")

        with src.open("rb") as rf, dst.open("wb") as wf:
            while chunk := rf.read(chunk_size):
                wf.write(chunk)
    elif src.is_dir():
        if dst.is_file():
            raise NotADirectoryError(f"{dst.as_uri()} is a file")

        for entry in src.rglob("*"):
            if entry.is_dir():
                continue

            entry = entry.relative_to(src)
            prefix = entry.parent
            (dst / prefix).mkdir(parents=True, exist_ok=True)

            rf = (src / entry).open("rb")
            wf = (dst / entry).open("wb")

            with rf, wf:
                while chunk := rf.read(chunk_size):
                    wf.write(chunk)
    else:
        raise RuntimeError(f"unexpected storage object: {src.as_uri()}")


def unlink(target: Path) -> None:
    """
    simplified remove interface.

    Args:
        target: path of file or directory.

    Note:
        if `target` is a directory, all child entries in
        directory will removed recursively.
    """

    target = target.resolve()

    if target.is_dir():
        files: List[Path] = []
        dirs: Set[Path] = set()
        for entry in target.rglob("*"):
            if entry.is_dir():
                continue

            if entry.is_file():
                dirs.add(entry.parent)
                files.append(entry)
            else:
                raise RuntimeError(f"unexpected entry: {entry.as_uri()}")

        for f in files:
            f.unlink(missing_ok=True)

        _dirs = sorted(dirs, reverse=True, key=lambda s: len(str(s)))
        for d in _dirs:
            if d.is_dir():
                d.rmdir()

        if target.is_dir():
            target.rmdir()
    elif target.is_file():
        target.unlink(missing_ok=True)
    else:
        raise RuntimeError(f"unexpected storage object: {target.as_uri()}")
