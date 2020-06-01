import fcntl
import os
import tempfile
import warnings

from multiprocessing import RawArray, Value

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
        self.offsets = RawArray('i', [-1] * self.length)
        self.lengths = RawArray('i', [0] * self.length)
        _, self.data_file = tempfile.mkstemp(dir=self.dir)

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
        assert i >= 0 and i < self.length
        o = self.offsets[i]
        l = self.lengths[i]
        if l <= 0 or o < 0:
            return None

        with open(self.data_file, 'rb') as f:
            fcntl.fcntl(f.fileno(), fcntl.LOCK_SH)
            f.seek(o)
            data = f.read(l)
            fcntl.fcntl(f.fileno(), fcntl.LOCK_UN)
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
        assert i >= 0 and i < self.length

        o = self.offsets[i]
        l = self.lengths[i]
        if 0 < l or 0 <= o:
            return False

        fd = os.open(self.data_file, os.O_APPEND | os.O_WRONLY)
        fcntl.flock(fd, fcntl.LOCK_EX)
        with os.fdopen(fd, 'ab') as f:
            pos = f.tell()
            assert f.write(data) == len(data)
            f.flush()

            self.offsets[i] = pos
            self.lengths[i] = len(data)
            fcntl.flock(fd, fcntl.LOCK_UN)

            return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            # https://github.com/python/cpython/blob/3.8/Lib/tempfile.py#L437
            os.unlink(self.data_file)
            self.data_file = None

    @property
    def multiprocess_safe(self) -> bool:
        return True

    @property
    def multithread_safe(self) -> bool:
        return True
