import io
import os
import pathlib
import shutil
from typing import Optional

try:
    from pytorch_pfn_extras.profiler import record_function, record_iterable

except ImportError:

    # IF PPE is not available, wrap with noop
    def record_function(*args):
        def wrapper(f):
            return f
        return wrapper

    def record_iterable(tag, iter, *args):
        yield from iter


from .fs import FS, FileStat, format_repr


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
    def __init__(self, cwd=None, create=False, **_):
        super().__init__()

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

    @record_function("local-open")
    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        path = os.path.join(self.cwd, file_path)
        return io.open(path, mode,
                       buffering, encoding, errors,
                       newline, closefd, opener)

    def list(self, path: Optional[str] = '', recursive=False,
             detail=False):
        for e in record_iterable("local-list",
                                 self._list(path, recursive, detail)):
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

    @record_function("local-stat")
    def stat(self, path):
        path = os.path.join(self.cwd, path)
        return LocalFileStat(os.stat(path), path)

    def isdir(self, path: str):
        path = os.path.join(self.cwd, path)
        return os.path.isdir(path)

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        path = os.path.join(self.cwd, file_path)
        return os.mkdir(path, mode, *args, dir_fd=None)

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        path = os.path.join(self.cwd, file_path)
        return os.makedirs(path, mode, exist_ok)

    def exists(self, file_path: str):
        path = os.path.join(self.cwd, file_path)
        return os.path.exists(path)

    def rename(self, src, dst):
        s = os.path.join(self.cwd, src)
        d = os.path.join(self.cwd, dst)
        return os.rename(s, d)

    def remove(self, file_path: str, recursive=False):
        file_path = os.path.join(self.cwd, file_path)
        if recursive:
            return shutil.rmtree(file_path)
        if os.path.isdir(file_path):
            return os.rmdir(file_path)

        return os.remove(file_path)

    def glob(self, pattern: str):
        return [
            str(item.relative_to(self.cwd))
            for item in pathlib.Path(self.cwd).glob(pattern)]
