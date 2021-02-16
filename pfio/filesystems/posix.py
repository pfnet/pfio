import io
import os
import shutil

from pfio.filesystem import FileSystem
from pfio.io import FileStat, open_wrapper


class PosixFileStat(FileStat):
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


class PosixFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root=""):
        FileSystem.__init__(self, io_profiler, root)
        self.type = 'posix'

    def info(self):
        # this is a place holder
        info_str = 'Posix file system'
        return info_str

    @open_wrapper
    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        return io.open(file_path, mode,
                       buffering, encoding, errors,
                       newline, closefd, opener)

    def list(self, path_or_prefix: str = None, recursive=False):
        if recursive:
            path_or_prefix = path_or_prefix.rstrip("/")
            # plus 1 to include the trailing slash
            prefix_end_index = len(path_or_prefix) + 1
            yield from self._recursive_list(prefix_end_index, path_or_prefix)
        else:
            for file in os.scandir(path_or_prefix):
                yield file.name

    def _recursive_list(self, prefix_end_index: int, path: str):
        for file in os.scandir(path):
            yield file.path[prefix_end_index:]

            if file.is_dir():
                yield from self._recursive_list(prefix_end_index,
                                                file.path)

    def stat(self, path):
        return PosixFileStat(os.stat(path), path)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def isdir(self, file_path: str):
        return os.path.isdir(file_path)

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        return os.mkdir(file_path, mode, *args, dir_fd=None)

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        return os.makedirs(file_path, mode, exist_ok)

    def exists(self, file_path: str):
        return os.path.exists(file_path)

    def rename(self, src, dst):
        return os.rename(src, dst)

    def remove(self, file_path: str, recursive=False):
        if recursive:
            return shutil.rmtree(file_path)
        if os.path.isdir(file_path):
            return os.rmdir(file_path)

        return os.remove(file_path)
