import abc
from abc import abstractmethod
import six

from typing import Optional, Callable


class Cache(six.with_metaclass(abc.ABCMeta)):
    '''Abstract class to define Cache class interface

    This can be instance of ``collections.abc.Sequence`` but so far
    this will be just a single interface difinition.  Note that this
    is experimental feature.

    '''

    def __init__(self):
        pass

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def multiprocess_safe(self) -> bool:
        raise NotImplementedError()

    @property
    @abstractmethod
    def multithread_safe(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def put(self, i: int, data: bytes) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get(self, i: int) -> Optional[bytes]:
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


from chainerio.cache.naive import NaiveCache  # NOQA
from chainerio.cache.file_cache import FileCache  # NOQA