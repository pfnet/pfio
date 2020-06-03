import fcntl
import os
import tempfile
import warnings

from struct import pack, unpack, calcsize

from pfio import cache
from pfio.cache.file_cache import _DEFAULT_CACHE_PATH
import pickle


class MultiprocessFileCache(cache.Cache):

    def __init__(self, length, do_pickle=False,
                 dir=None, verbose=False):
        self.length = length
        self.do_pickle = do_pickle
        assert self.length > 0

        if dir is None:
            self.dir = _DEFAULT_CACHE_PATH
        else:
            self.dir = dir
        os.makedirs(self.dir, exist_ok=True)

        self.closed = False
        _, self.data_file = tempfile.mkstemp(dir=self.dir)
        indexfp, self.index_file = tempfile.mkstemp(dir=self.dir)

        # Initialize index file with index=0, size=-1
        buf = pack('Qq', 0, -1)
        self.buflen = calcsize('Qq')
        assert self.buflen == 16
        for i in range(self.length):
            offset = self.buflen * i
            r = os.pwrite(indexfp, buf, offset)
            assert r == self.buflen

    def get_offsets(self):
        fd = os.open(self.index_file, os.O_RDONLY)
        offsets = []
        for i in range(self.length):
            offset = self.buflen * i
            buf = os.pread(fd, self.buflen, offset)
            (o, l) = unpack('Qq', buf)
            offsets.append(o)
        return offsets

    def __len__(self):
        return self.length

    def get(self, i):
        if self.closed:
            return
        data = self._get(i)
        if self.do_pickle and data:
            data = pickle.loads(data)
        return data

    def _get(self, i):
        assert 0 <= i < self.length

        offset = self.buflen * i
        index_fd = os.open(self.index_file, os.O_RDONLY)
        fcntl.flock(index_fd, fcntl.LOCK_SH)
        buf = os.pread(index_fd, self.buflen, offset)
        (o, l) = unpack('Qq', buf)
        if l < 0 or o < 0:
            fcntl.flock(index_fd, fcntl.LOCK_UN)
            return None

        data_fd = os.open(self.data_file, os.O_RDONLY)
        with os.fdopen(data_fd, 'rb') as f:
            f.seek(o)
            data = f.read(l)
        fcntl.flock(index_fd, fcntl.LOCK_UN)
        assert len(data) == l
        return data

    def put(self, i, data):
        try:
            if self.do_pickle:
                data = pickle.dumps(data)
            return self._put(i, data)

        except OSError as ose:
            # Disk full (ENOSPC) possibly by cache; just warn and keep running
            if ose.errno == 28:
                warnings.warn(ose.strerror, RuntimeWarning)
                return False
            else:
                raise ose

    def _put(self, i, data):
        if self.closed:
            return
        assert 0 <= i < self.length

        offset = self.buflen * i
        index_fd = os.open(self.index_file, os.O_RDWR)
        fcntl.flock(index_fd, fcntl.LOCK_EX)
        buf = os.pread(index_fd, self.buflen, offset)
        (o, l) = unpack('Qq', buf)

        if l >= 0 and o >= 0:
            # Already data exists
            fcntl.flock(index_fd, fcntl.LOCK_UN)
            return False

        data_fd = os.open(self.data_file, os.O_APPEND | os.O_WRONLY)
        with os.fdopen(data_fd, 'ab') as f:
            pos = f.tell()

            # Write the position to the index file
            buf = pack('Qq', pos, len(data))
            r = os.pwrite(index_fd, buf, offset)
            assert r == self.buflen

            # Write the data
            assert f.write(data) == len(data)

        fcntl.flock(index_fd, fcntl.LOCK_UN)
        return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        # https://github.com/python/cpython/blob/3.8/Lib/tempfile.py#L437
        self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            os.unlink(self.data_file)
            os.unlink(self.index_file)
            self.data_file = None

    @property
    def multiprocess_safe(self) -> bool:
        return True

    @property
    def multithread_safe(self) -> bool:
        return True
