import logging
import os
import time

from typing import Optional

import urllib3
import urllib3.exceptions

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class HTTPConnector(object):
    def __init__(self, url: str, bearer_token_path: Optional[str] = None):
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

    def put(self, suffix: str, data: bytes) -> bool:
        self._checkconn()

        try:
            res = self.conn.urlopen("PUT",
                                    url=self.url + suffix,
                                    headers=self._header_with_token(),
                                    body=data)
        except urllib3.exceptions.RequestError as e:
            logger.warning("put: {}".format(e))
            return False

        if res.status == 201:
            return True
        else:
            logger.warning("put: unexpected status code {}".format(res.status))
            return False

    def get(self, suffix: str) -> Optional[bytes]:
        self._checkconn()

        try:
            res = self.conn.urlopen("GET",
                                    url=self.url + suffix,
                                    headers=self._header_with_token())
        except urllib3.exceptions.RequestError as e:
            logger.warning("get: {}".format(e))
            return None

        if res.status == 200:
            return res.data
        elif res.status == 404:
            return None
        else:
            logger.warning("get: unexpected status code {}".format(res.status))
            return None

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
