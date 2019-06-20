from chainerio.filesystem import FileSystem
from chainerio.io import open_wrapper
import io
import os
import shutil
from chainerio.profiler import profiler_decorator


class PosixFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root=""):
        FileSystem.__init__(self, io_profiler, root)
        self.type = 'posix'

    def info(self):
        # this is a place holder
        info_str = 'Posix file system'
        return info_str

    @profiler_decorator
    @open_wrapper
    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        return io.open(file_path, mode,
                       buffering, encoding, errors,
                       newline, closefd, opener)

    @profiler_decorator
    def list(self, path_or_prefix: str = None):
        for file in os.scandir(path_or_prefix):
            yield file.name

    @profiler_decorator
    def stat(self, path):
        return os.stat(path)

    @profiler_decorator
    def close(self):
        pass

    @profiler_decorator
    def __enter__(self):
        return self

    @profiler_decorator
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    @profiler_decorator
    def isdir(self, file_path: str):
        return os.path.isdir(file_path)

    @profiler_decorator
    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        return os.mkdir(file_path, mode, *args, dir_fd=None)

    @profiler_decorator
    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        return os.makedirs(file_path, mode, exist_ok)

    @profiler_decorator
    def exists(self, file_path: str):
        return os.path.exists(file_path)

    @profiler_decorator
    def rename(self, src, dst):
        return os.rename(src, dst)

    @profiler_decorator
    def remove(self, file_path: str, recursive=False):
        if recursive:
            return shutil.rmtree(file_path)
        if os.path.isdir(file_path):
            return os.rmdir(file_path)

        return os.remove(file_path)
