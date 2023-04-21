import pickle
from typing import Any

from pfio.cache import Cache
from pfio.util import HTTPConnector


class HTTPCache(Cache):
    """HTTP-based cache system

    Stores cache data in an HTTP server with ``PUT`` and ``GET`` methods. Each
    cache entry corresponds to url suffixed by index ``i``.

    Arguments:
        length (int):
            Length of the cache.

        url (string):
            Prefix url of cache entries. Each entry corresponds to the url
            suffixed by each index. A user must specify url as globally
            identical across the cache system in the server side, because
            ``HTTPCache`` doesn't suffix the url by user or dataset
            information. Therefore, a user should include user and dataset in
            the url to avoid conflicting the cache entry.

            For example, let's assume that given url is
            ``http://cache.example.com/some/{user}/{dataset-id}/``. Here,
            ``put(123)`` and ``get(123)`` correspond to
            ``http://cache.example.com/some/{user}/{dataset-id}/123``.

        bearer_token_path (string):
            Path to HTTP bearer token if authorization required. ``HTTPCache``
            supports refresh of bearer token by periodical reloading.

        do_pickle (bool):
            Do automatic pickle and unpickle inside the cache.

    .. note:: This feature is experimental.

    """

    def __init__(self,
                 length: int,
                 url: str,
                 bearer_token_path=None,
                 do_pickle=False):
        super().__init__()

        self.length = length
        assert self.length > 0

        self.connector = HTTPConnector(url, bearer_token_path)
        self.do_pickle = do_pickle

    def __len__(self):
        return self.length

    @property
    def multiprocess_safe(self):
        return True

    @property
    def multithread_safe(self):
        return True

    def put(self, i: int, data: Any):
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))
        if self.do_pickle:
            data = pickle.dumps(data)

        return self.connector.put(str(i), data)

    def get(self, i: int) -> Any:
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))

        data = self.connector.get(str(i))

        if self.do_pickle and data is not None:
            return pickle.loads(data)
        else:
            return data
