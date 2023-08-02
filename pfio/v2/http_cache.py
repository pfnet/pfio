import io
from types import TracebackType
from typing import Any, Iterator, Optional, Type, Union

from pfio.cache import HTTPConnector

from .fs import FS, FileStat


class HTTPCachedFS(FS):
    """HTTP-based cache system

    Stores cache data in an HTTP server with ``PUT`` and ``GET`` methods. Each
    cache entry corresponds to url suffixed by _canonical_name in
    :py:class:`pfio.v2.fs.FS`.

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

        max_cache_size (int):
            Files larger than max_cache_size will not be cached.
            max_cache_size is 1 GiB by default.

        bearer_token_path (string):
            Path to HTTP bearer token if authorization required.
            ``HTTPCachedFS`` supports refresh of bearer token by periodical
            reloading.

    .. note:: This feature is experimental.

    """

    def __init__(self,
                 url: str,
                 fs: FS,
                 max_cache_size: int = 1024 * 1024 * 1024,
                 bearer_token_path: Optional[str] = None):
        assert not isinstance(fs, HTTPCachedFS)
        super().__init__()

        self.fs = fs
        self.max_cache_size = max_cache_size
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
                                    self.max_cache_size, args, kwargs)
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

    def _canonical_name(self, file_path: str) -> str:
        # Don't add httpcache in normpath
        return self.fs._canonical_name(file_path)


class _HTTPCacheIOBase(io.RawIOBase):
    def __init__(self,
                 file_path: str,
                 conn: HTTPConnector,
                 fs: FS,
                 max_cache_size: int,
                 open_args: Any, open_kwargs: dict):
        super(_HTTPCacheIOBase, self).__init__()

        self.file_path = file_path
        self.conn = conn
        self.fs = fs
        self.max_cache_size = max_cache_size
        self.open_args = open_args
        self.open_kwargs = open_kwargs

        self.cache_path = self.fs._canonical_name(self.file_path)
        self.whole_file: Optional[bytes] = None
        self.pos: Optional[int] = None
        self.fp: Optional[io.RawIOBase] = None

        self._closed = False

    def _load_file(self):
        if self.whole_file is not None:
            return

        if self.fp is not None:
            return

        # Try HTTPCache.
        data = self.conn.get(self.cache_path)
        if data is not None:
            self.whole_file = data
            self.pos = 0
            return

        # Check size in underlying fs.
        stat = self.fs.stat(self.file_path)
        if stat.size < self.max_cache_size:
            # The filesize is smaller than max_cache_size so let's cache it

            # Read whole file
            with self.fs.open(self.file_path,
                              *self.open_args, **self.open_kwargs) as fp:
                self.whole_file = fp.read(-1)
            self.pos = 0

            # Put it to HTTPCache.
            self.conn.put(self.cache_path, self.whole_file)
        else:
            # The file is larger than max_cache_size
            print(
                "HTTPCachedFS: Too big data ({} bytes), skipping cache".format(
                    stat.size
                )
            )

            # Access through underlying filesystem
            self.fp = self.fs.open(self.file_path,
                                   *self.open_args, **self.open_kwargs)

    def read(self, size=-1) -> bytes:
        self._load_file()
        if self.whole_file is not None:
            assert self.pos is not None

            if len(self.whole_file) <= self.pos:
                return b''
            elif size <= 0:
                data = self.whole_file[self.pos:]
            else:
                end = min(self.pos + size, len(self.whole_file))
                data = self.whole_file[self.pos:end]

            self.pos += len(data)
            return data
        elif self.fp is not None:
            data_from_fp = self.fp.read(size)
            if data_from_fp is not None:
                return data_from_fp

        print("HTTPCachedFS: failed to read from backend fs")
        return b''

    def readline(self):
        raise NotImplementedError()

    def close(self):
        self._closed = True
        if self.fp is not None:
            self.fp.close()

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
        self._load_file()

        if self.pos is not None:
            return self.pos
        else:
            assert self.fp is not None
            return self.fp.tell()

    def truncate(self, size=None):
        raise io.UnsupportedOperation('truncate')

    def seek(self, pos, whence=io.SEEK_SET):
        self._load_file()

        if self.pos is not None:
            if whence in [0, io.SEEK_SET]:
                pass
            elif whence in [1, io.SEEK_CUR]:
                pos += self.pos
            elif whence in [2, io.SEEK_END]:
                pos += len(self.whole_file)
            else:
                raise ValueError('Wrong whence value: {}'.format(whence))

            if pos < 0:
                raise OSError(22, "[Errno 22] Invalid argument")

            self.pos = pos
            return self.pos
        else:
            assert self.fp is not None
            return self.fp.seek(pos, whence)

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
