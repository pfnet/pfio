import logging
import os
import pickle
import time

import urllib3
import urllib3.exceptions

from pfio.cache import Cache

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


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

        if url.endswith("/"):
            self.url = url
        else:
            self.url = url + "/"

        if bearer_token_path is not None:
            self.bearer_token_path = bearer_token_path
        else:
            self.bearer_token_path = os.getenv("PFIO_HTTP_BEARER_TOKEN_PATH")

        if self.bearer_token_path is not None:
            self._token_read_now()

        self.do_pickle = do_pickle

        self.conn = None
        self._prepare_conn()

        self.pid = os.getpid()

    @property
    def is_forked(self):
        return self.pid != os.getpid()

    def _checkconn(self):
        if self.is_forked or self.conn is None:
            self._prepare_conn()
            self.pid = os.getpid()

    def _prepare_conn(self):
        # Allow redirect or retry once
        self.conn = urllib3.poolmanager.PoolManager(retries=1, timeout=3)

    def __getstate__(self):
        state = self.__dict__.copy()
        state['conn'] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state

    def __len__(self):
        return self.length

    @property
    def multiprocess_safe(self):
        return True

    @property
    def multithread_safe(self):
        return True

    def put(self, i, data):
        self._checkconn()
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))
        if self.do_pickle:
            data = pickle.dumps(data)

        try:
            res = self.conn.urlopen("PUT",
                                    url=self._url(i),
                                    headers=self._header_with_token(),
                                    body=data)
        except urllib3.exceptions.RequestError as e:
            logger.warning("put: {}".format(e))
            return False

        if res.status == 201:
            return True

        logger.warning("put: unexpected status code {}".format(res.status))
        return False

    def get(self, i):
        self._checkconn()
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))

        try:
            res = self.conn.urlopen("GET",
                                    url=self._url(i),
                                    headers=self._header_with_token())
        except urllib3.exceptions.RequestError as e:
            logger.warning("get: {}".format(e))
            return None

        if res.status == 200:
            if self.do_pickle:
                return pickle.loads(res.data)
            else:
                return res.data
        elif res.status == 404:
            return None

        logger.warning("get: unexpected status code {}".format(res.status))
        return None

    def _url(self, i) -> str:
        return self.url + str(i)

    def _header_with_token(self) -> dict:
        if self.bearer_token_path is None:
            return {}
        else:
            if time.time() - self.bearer_token_updated > 1:
                self._token_read_now()
            return {
                "Authorization": f"Bearer {self.bearer_token}"
            }

    def _token_read_now(self):
        with open(self.bearer_token_path, "r") as f:
            self.bearer_token = f.read()
            self.bearer_token_updated = time.time()
