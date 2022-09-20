import os
import pickle
import time

import urllib3

from pfio.cache import Cache


class HTTPCache(Cache):
    def __init__(self, length: int, url: str, bearer_token_path=None, do_pickle=False):
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

        self.conn = urllib3.poolmanager.PoolManager()

    def __len__(self):
        return self.length

    @property
    def multiprocess_safe(self):
        return True

    @property
    def multithread_safe(self):
        return True

    def put(self, i, data):
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))
        if self.do_pickle:
            data = pickle.dumps(data)

        res = self.conn.urlopen("PUT", url=self._url(i), headers=self._header_with_token(), body=data)
        return res.status == 201

    def get(self, i):
        if i < 0 or self.length <= i:
            raise IndexError("index {} out of range ([0, {}])"
                             .format(i, self.length - 1))

        res = self.conn.urlopen("GET", url=self._url(i), headers=self._header_with_token())
        if res.status != 200 or res.data is None:
            return None

        if self.do_pickle:
            return pickle.loads(res.data)
        else:
            return res.data

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
