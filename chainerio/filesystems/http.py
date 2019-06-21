from chainerio.filesystem import FileSystem
from chainerio.io import open_wrapper

import io
import sys
from urllib import request

from chainerio.profiler import profiling_decorator


class HttpFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root=""):
        FileSystem.__init__(self, io_profiler, root)
        self.type = "url"
        self.url = None

    @profiling_decorator
    @open_wrapper
    def open(self, url, mode='rb',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):
        self.url = url
        return self._download(url)

    def _download(self, url):
        sys.stderr.write('Downloading from {}...\n'.format(url))
        sys.stderr.flush()
        url_request = request.urlopen(url)

        return url_request

    @profiling_decorator
    def close(self):
        pass

    @profiling_decorator
    def info(self):
        # this is a place holder
        info_str = 'Http file system'
        if None is not self.url:
            info_str += ', the URL is {}'.format(self.url)
        return info_str

    @profiling_decorator
    def list(self, path_or_prefix: str = None):
        raise io.UnsupportedOperation("http filesystem does not support list")

    @profiling_decorator
    def stat(self, path):
        raise io.UnsupportedOperation("http filesystem does not support stat")

    @profiling_decorator
    def isdir(self, file_path: str):
        raise io.UnsupportedOperation("http filesystem does not support isdir")

    @profiling_decorator
    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        raise io.UnsupportedOperation("http filesystem does not support mkdir")

    @profiling_decorator
    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        raise io.UnsupportedOperation(
            "http filesystem does not support makedirs")

    @profiling_decorator
    def exists(self, file_path: str):
        raise io.UnsupportedOperation(
            "http filesystem does not support exists")

    @profiling_decorator
    def __enter__(self):
        return self

    @profiling_decorator
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def read(self, url):
        return self._download(url).read()

    def write(self, url, content, mode='wb'):
        raise io.UnsupportedOperation

    @profiling_decorator
    def rename(self, src, dst):
        raise io.UnsupportedOperation

    @profiling_decorator
    def remove(self, file_path, recursive=False):
        raise io.UnsupportedOperation
