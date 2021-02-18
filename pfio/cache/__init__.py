import abc
from abc import abstractmethod
from typing import Callable

from pfio._typing import Optional


class Cache(abc.ABC):
    '''Abstract class to define Cache class interface

    This can be instance of ``collections.abc.Sequence`` but so far
    this will be just a single interface difinition.  Note that this
    is experimental feature.

    '''

    def __init__(self):
        pass

    @abstractmethod
    def __len__(self) -> int:
        "Returns the length of the cache data"
        raise NotImplementedError()

    @property
    @abstractmethod
    def multiprocess_safe(self) -> bool:
        "Returns multiprocess safety."
        raise NotImplementedError()

    @property
    @abstractmethod
    def multithread_safe(self) -> bool:
        "Returns multithread safety."
        raise NotImplementedError()

    @abstractmethod
    def put(self, i: int, data: bytes) -> bool:
        "Puts the bytes to the cache. No overwrite nor deletion supported."
        raise NotImplementedError()

    @abstractmethod
    def get(self, i: int) -> Optional[bytes]:
        "Tries to get the data from cache."
        raise NotImplementedError()

    def get_and_cache(self, i, backend_get: Callable[[int], bytes]) -> bytes:
        '''Get data from cache, otherwise from backend with caching

        First try to get data from cache. If not found, it gets data
        from backend callable with the result stored in cache.

        '''
        data = self.get(i)
        if not data:
            data = backend_get(i)
            self.put(i, data)
        return data


from pfio.cache.file_cache import FileCache  # NOQA
from pfio.cache.multiprocess_file_cache import MultiprocessFileCache  # NOQA
from pfio.cache.naive import NaiveCache  # NOQA
