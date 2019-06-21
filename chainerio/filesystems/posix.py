from chainerio.filesystem import FileSystem
from chainerio.io import open_wrapper
import io
import os
import shutil
from chainerio.profiler import profiling_decorator


class PosixFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root=""):
        FileSystem.__init__(self, io_profiler, root)
        self.type = 'posix'

    def info(self):
        # this is a place holder
        info_str = 'Posix file system'
        return info_str

    @open_wrapper
    @profiling_decorator
    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        return io.open(file_path, mode,
                       buffering, encoding, errors,
                       newline, closefd, opener)

    @profiling_decorator
    def list(self, path_or_prefix: str = None):
        for file in os.scandir(path_or_prefix):
            yield file.name

    @profiling_decorator
    def stat(self, path):
        return os.stat(path)

    @profiling_decorator
    def close(self):
        pass

    @profiling_decorator
    def __enter__(self):
        return self

    @profiling_decorator
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @profiling_decorator
    def isdir(self, file_path: str):
        return os.path.isdir(file_path)

    @profiling_decorator
    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        return os.mkdir(file_path, mode, *args, dir_fd=None)

    @profiling_decorator
    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        return os.makedirs(file_path, mode, exist_ok)

    @profiling_decorator
    def exists(self, file_path: str):
        return os.path.exists(file_path)

    @profiling_decorator
    def rename(self, src, dst):
        return os.rename(src, dst)

    @profiling_decorator
    def remove(self, file_path: str, recursive=False):
        if recursive:
            return shutil.rmtree(file_path)
        if os.path.isdir(file_path):
            return os.rmdir(file_path)

        return os.remove(file_path)
