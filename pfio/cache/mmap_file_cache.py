import io
import mmap
import os
import pickle
from struct import calcsize, unpack

from pfio import cache
from pfio.cache.file_cache import _default_cache_path


class ReadOnlyFileCache(cache.Cache):
    '''Read only cache system with local filesystem

    Example::

      c = ReadOnlyFileCache(65536, dir="path/to/cache")
      c.preload("filename")

    Arguments:
        length (int): Length of the cache array.

        do_pickle (bool):
            Do automatic pickle and unpickle inside the cache.

        verbose (bool):
            Print detailed logs of the cache.

    '''

    def __init__(self, length, do_pickle=False, dir=None, verbose=False):
        self.length = length
        self.do_pickle = do_pickle
        if self.length <= 0 or (2 ** 64) <= self.length:
            raise ValueError("length has to be between 0 and 2^64")

        if dir is None:
            self.dir = _default_cache_path()
        else:
            self.dir = dir
        cache.file_cache._check_local(self.dir)

        self.buflen = calcsize('Qq')
        assert self.buflen == 16

        self.closed = True
        self.verbose = verbose

    def __len__(self):
        return self.length

    @property
    def frozen(self):
        return True

    @property
    def multiprocess_safe(self):
        return True

    @property
    def multithread_safe(self):
        return True

    def get(self, i):
        if self.closed:
            return
        data = self._get(i)
        if self.do_pickle and data:
            data = pickle.loads(data)
        return data

    def _get(self, i):
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))

        (o, l) = self.offset_list[i]
        return self.mreg[o:(o+l)]

    def put(self, i, data):
        raise io.UnsupportedOperation('read only cache')

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        if not self.closed:
            self.mreg.close()
            self.fp.close()
            self.closed = True

    def preload(self, name):
        '''Load the cache saved by ``preserve()``

        ``cache_path`` is the path to the persistent file. To use cache
        in ``multiprocessing`` environment, call this method at every
        forked process, except the process that called ``preserve()``.
        After the preload, no data can be added to the cache.

        When it succeeds, it returns ``True``.
        If there is no cache file with the specified name in
        the cache directory, it will do nothing but return ``False``.

        Returns:
            bool: Returns True if succeed.

        .. note:: This feature is experimental.

        '''
        filename = os.path.join(self.dir, name)
        self.fp = open(filename, 'rb')

        self.offset_list = []
        for i in range(self.length):
            offset = self.buflen * i
            buf = os.pread(self.fp.fileno(), self.buflen, offset)
            (o, l) = unpack('Qq', buf)
            if l < 0 or o < 0:
                raise RuntimeError("Incomplete cache file")

            self.offset_list.append((o, l))

        assert self.length == len(self.offset_list)

        # mmaped region
        self.mreg = mmap.mmap(self.fp.fileno(), 0, prot=mmap.PROT_READ)
        self.closed = False
        return True

    def preserve(self, name, overwrite=False):
        raise io.UnsupportedOperation('read only')
