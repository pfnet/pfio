from pfio.filesystem import FileSystem
from pfio.io import FileStat
from pfio.io import open_wrapper
import io
import os
import shutil


class PosixFileStat(FileStat):
    def __init__(self, _stat, filename):
        self.filename = filename
        self.last_modified = _stat.st_mtime
        self.last_accessed = _stat.st_atime
        self.created = _stat.st_ctime
        self.mode = _stat.st_mode
        self.size = _stat.st_size
        self.owner = _stat.st_uid
        self.group = _stat.st_gid
        self.inode = _stat.st_ino
        self.device = _stat.st_dev
        self.n_link = _stat.st_nlink


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
