import io
from types import TracebackType
from typing import Any, Iterator, Optional, Type, Union

from pfio.cache import HTTPConnector

from .fs import FS, FileStat


class HTTPCachedFS(FS):
    """HTTP-based cache system

    Stores cache data in an HTTP server with ``PUT`` and ``GET`` methods. Each
    cache entry corresponds to url suffixed by normalized paths (``normpath``).

    Arguments:
        url (string):
            Prefix url of cache entries. Each entry corresponds to the url
            suffixed by each normalized paths.

        fs (pfio.v2.FS):
            Underlying filesystem.

            Read operations will be hooked by HTTPCachedFS to send a request to
            the cache system. If the object is found in cache, the object will
            be returned from cache without requesting to underlying fs.
            Therefore, after the update of file in underlying fs, users have to
            update url to avoid reading old data from the cache.

            Other operations including write will not be hooked. It will be
            transferred to underlying filesystem immediately.

        bearer_token_path (string):
            Path to HTTP bearer token if authorization required.
            ``HTTPCachedFS`` supports refresh of bearer token by periodical
            reloading.

    .. note:: This feature is experimental.

    """

    def __init__(self,
                 url: str,
                 fs: FS,
                 bearer_token_path: Optional[str] = None):
        super().__init__()
        self.fs = fs
        self.conn = HTTPConnector(url, bearer_token_path)
        if url.endswith("/"):
            self.url = url
        else:
            self.url = url + "/"

    def open(self,
             file_path: str,
             mode: str = 'rb',
             *args, **kwargs) -> io.IOBase:
        if 'r' in mode:
            kwargs['mode'] = mode
            return _HTTPCacheIOBase(file_path, self.conn, self.fs,
                                    args, kwargs)
        else:
            return self.fs.open(file_path, mode, *args, **kwargs)

    def _reset(self):
        self.fs._reset()

    def list(self, *args, **kwargs) -> Iterator[Union[FileStat, str]]:
        return self.fs.list(*args, **kwargs)

    def stat(self, *args, **kwargs) -> FileStat:
        return self.fs.stat(*args, **kwargs)

    def isdir(self, *args, **kwargs) -> bool:
        return self.fs.isdir(*args, **kwargs)

    def mkdir(self, *args, **kwargs) -> None:
        return self.fs.mkdir(*args, **kwargs)

    def makedirs(self, *args, **kwargs) -> None:
        return self.fs.makedirs(*args, **kwargs)

    def exists(self, *args, **kwargs) -> bool:
        return self.fs.exists(*args, **kwargs)

    def rename(self, *args, **kwargs) -> None:
        return self.fs.rename(*args, **kwargs)

    def remove(self, *args, **kwargs) -> None:
        return self.fs.remove(*args, **kwargs)

    def glob(self, pattern: str) -> Iterator[Union[FileStat, str]]:
        return self.fs.glob(pattern)

    def normpath(self, file_path: str) -> str:
        # Don't add httpcache in normpath
        return self.fs.normpath(file_path)


class _HTTPCacheIOBase(io.RawIOBase):
    def __init__(self,
                 file_path: str,
                 conn: HTTPConnector,
                 fs: FS,
                 open_args: Any, open_kwargs: dict):
        super(_HTTPCacheIOBase, self).__init__()

        self.file_path = file_path
        self.conn = conn
        self.fs = fs
        self.open_args = open_args
        self.open_kwargs = open_kwargs

        self.cache_path = self.fs.normpath(self.file_path)
        self.whole_file: Optional[bytes] = None
        self.pos = 0
        self._closed = False

    def _load_file(self):
        if self.whole_file is not None:
            return

        data = self.conn.get(self.cache_path)
        if data is not None:
            self.whole_file = data
            return

        with self.fs.open(self.file_path,
                          *self.open_args, **self.open_kwargs) as f:
            self.whole_file = f.read(-1)

        if 1024 * 1024 * 1024 <= len(self.whole_file):
            print(
                "HTTPCachedFS: Too big data ({} bytes)".format(
                    len(self.whole_file)
                )
            )
            return

        self.conn.put(self.cache_path, self.whole_file)

    def read(self, size=-1) -> bytes:
        self._load_file()
        if self.whole_file is None:
            print("HTTPCachedFS: failed to read from backend fs")
            return b''

        if len(self.whole_file) <= self.pos:
            return b''
        elif size <= 0:
            data = self.whole_file[self.pos:]
        else:
            end = min(self.pos + size, len(self.whole_file))
            data = self.whole_file[self.pos:end]

        self.pos += len(data)
        return data

    def readline(self):
        raise NotImplementedError()

    def close(self):
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]):
        self.close()

    def flush(self):
        pass

    @property
    def closed(self):
        return self._closed

    def isatty(self):
        return False

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def truncate(self, size=None):
        raise io.UnsupportedOperation('truncate')

    def seek(self, pos, whence=io.SEEK_SET):
        if whence in [0, io.SEEK_SET]:
            if pos < 0:
                raise OSError(22, "[Errno 22] Invalid argument")
        elif whence in [1, io.SEEK_CUR]:
            pos += self.pos
        elif whence in [2, io.SEEK_END]:
            self._load_file()
            pos += len(self.whole_file)
        else:
            raise ValueError('Wrong whence value: {}'.format(whence))

        if pos < 0:
            raise OSError(22, "[Errno 22] Invalid argument")
        self.pos = pos
        return self.pos

    def writable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('not writable')

    def readall(self):
        return self.read(-1)

    def readinto(self, b):
        buf = self.read(len(b))
        b[:len(buf)] = buf
        return len(buf)
