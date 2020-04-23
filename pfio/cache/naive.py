import contextlib
import threading

from pfio.cache import Cache


class NaiveCache(Cache):
    '''Naive on-memory cache just with dict.'''

    def __init__(self, length, multithread_safe=False, do_pickle=False):
        self._multithread_safe = multithread_safe
        self.length = length
        assert self.length > 0

        if self._multithread_safe:
            self.lock = threading.Lock()
        else:
            # Use contextlib.nullcontext() when Python 3.6 is dropped.
            self.lock = contextlib.suppress()

        self.data = [None for _ in range(self.length)]

    def __len__(self):
        return self.length

    @property
    def multiprocess_safe(self):
        return False

    @property
    def multithread_safe(self):
        return self._multithread_safe

    def put(self, i, data):
        with self.lock:
            if self.data[i] is None:
                self.data[i] = data
                return True
            return False

    def get(self, i):
        with self.lock:
            return self.data[i]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass
