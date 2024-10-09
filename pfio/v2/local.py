import io
import os
import pathlib
import shutil
from typing import Optional

from pfio._profiler import record, record_iterable

from .fs import FS, FileStat, format_repr


class LocalProfileIOWrapper:
    def __init__(self, fp):
        self.fp = fp

    def __enter__(self):
        self.fp.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with record("pfio.v2.Local:exit-context", trace=True):
            self.fp.__exit__(exc_type, exc_value, traceback)

    def __getattr__(self, name):
        attr = getattr(self.fp, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                with record(f"pfio.v2.Local:{attr.__name__}", trace=True):
                    return attr(*args, **kwargs)
            return wrapper
        else:
            return attr


class LocalFileStat(FileStat):
    """Detailed information of a POSIX file

    The information of file/directory is obtained through the `os.stat`.

    Attributes:
        filename (str): Derived from `~FileStat`.
        last_modified (float): Derived from `~FileStat`.
            ``os.stat_result.st_mtime``.
        last_accessed (float): ``os.stat_result.st_atime``.
        created (float): ``os.stat_result.st_ctime``.
        last_modified_ns (int): ``os.stat_result.st_mtime_ns``.
        last_accessed_ns (int): ``os.stat_result.st_atime_ns``.
        created_ns (float): ``os.stat_result.st_ctime``.
        mode (int): Derived from `~FileStat`. ``os.stat_result.st_mode``.
        size (int): Derived from `~FileStat`. ``os.stat_result.st_size``.
        owner (int): UID of owner in integer.
        group (int): GID of the file in integer.
        inode (int): ``os.stat_result.st_ino``.
        device (int): ``os.stat_result.st_dev``.
        nlink (int): ``os.stat_result.st_nlink``.
    """

    def __init__(self, _stat, filename):
        keys = (('last_modified', 'st_mtime'),
                ('last_accessed', 'st_atime'),
                ('last_modified_ns', 'st_mtime_ns'),
                ('last_accessed_ns', 'st_atime_ns'),
                ('created', 'st_ctime'), ('created_ns', 'st_ctime_ns'),
                ('mode', 'st_mode'), ('size', 'st_size'), ('uid', 'st_uid'),
                ('gid', 'st_gid'), ('ino', 'st_ino'), ('dev', 'st_dev'),
                ('nlink', 'st_nlink'))
        for k, ksrc in keys:
            setattr(self, k, getattr(_stat, ksrc))
        self.filename = filename


class Local(FS):
    def __init__(self, cwd=None, trace=False, create=False, **_):
        super().__init__()

        self.trace = trace

        if cwd is None:
            self._cwd = ''
        else:
            self._cwd = cwd

        if not self.isdir(''):
            if create:
                # Since this process (isdir -> makedirs) is not atomic,
                # makedirs can conflict in case of a parallel workload.
                os.makedirs(self._cwd, exist_ok=True)
            else:
                raise ValueError('{} must be a directory'.format(self._cwd))

    @property
    def cwd(self):
        if self._cwd:
            return self._cwd

        return os.getcwd()

    @cwd.setter
    def cwd(self, value: str):
        self._cwd = value

    def _reset(self):
        pass

    def __repr__(self):
        return format_repr(
            Local,
            {
                "cwd": self._cwd,
            },
        )

    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        with record("pfio.v2.Local:open", trace=self.trace):
            path = os.path.join(self.cwd, file_path)

            fp = io.open(path, mode,
                         buffering, encoding, errors,
                         newline, closefd, opener)

            # Add ppe recorder to io class methods (e.g. read, write)
            if self.trace:
                return LocalProfileIOWrapper(fp)
            else:
                return fp

    def list(self, path: Optional[str] = '', recursive=False,
             detail=False):
        for e in record_iterable("pfio.v2.Local:list",
                                 self._list(path, recursive, detail),
                                 trace=self.trace):
            yield e

    def _list(self, path: Optional[str] = '', recursive=False,
              detail=False):
        path_or_prefix = os.path.join(self.cwd,
                                      "" if path is None else path)

        if recursive:
            path_or_prefix = path_or_prefix.rstrip("/")
            # plus 1 to include the trailing slash
            prefix_end_index = len(path_or_prefix) + 1
            yield from self._recursive_list(prefix_end_index,
                                            path_or_prefix, detail)
        else:
            for e in os.scandir(path_or_prefix):
                # ls -F
                if detail:
                    yield LocalFileStat(e.stat(), e.name)
                elif e.is_dir():
                    yield e.name + '/'
                else:
                    yield e.name

    def _recursive_list(self, prefix_end_index: int, path: str,
                        detail: bool):
        for e in os.scandir(path):
            # ls -F
            if detail:
                yield LocalFileStat(e.stat(), e.name)
            elif e.is_dir():
                yield e.path[prefix_end_index:] + '/'
            else:
                yield e.path[prefix_end_index:]

            if e.is_dir():
                yield from self._recursive_list(prefix_end_index,
                                                e.path, detail)

    def stat(self, path):
        with record("pfio.v2.Local:stat", trace=self.trace):
            path = os.path.join(self.cwd, path)
            return LocalFileStat(os.stat(path), path)

    def isdir(self, path: str):
        path = os.path.join(self.cwd, path)
        return os.path.isdir(path)

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        with record("pfio.v2.Local:mkdir", trace=self.trace):
            path = os.path.join(self.cwd, file_path)
            return os.mkdir(path, mode, *args, dir_fd=None)

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        with record("pfio.v2.Local:makedirs", trace=self.trace):
            path = os.path.join(self.cwd, file_path)
            return os.makedirs(path, mode, exist_ok)

    def exists(self, file_path: str):
        with record("pfio.v2.Local:exists", trace=self.trace):
            path = os.path.join(self.cwd, file_path)
            return os.path.exists(path)

    def rename(self, src, dst):
        with record("pfio.v2.Local:rename", trace=self.trace):
            s = os.path.join(self.cwd, src)
            d = os.path.join(self.cwd, dst)
            return os.rename(s, d)

    def remove(self, file_path: str, recursive=False):
        with record("pfio.v2.Local:remove", trace=self.trace):
            file_path = os.path.join(self.cwd, file_path)
            if recursive:
                return shutil.rmtree(file_path)
            if os.path.isdir(file_path):
                return os.rmdir(file_path)

            return os.remove(file_path)

    def glob(self, pattern: str):
        with record("pfio.v2.Local:glob", trace=self.trace):
            return [
                str(item.relative_to(self.cwd))
                for item in pathlib.Path(self.cwd).glob(pattern)]

    def _canonical_name(self, file_path: str) -> str:
        return "file:/" + os.path.normpath(os.path.join(self.cwd, file_path))
