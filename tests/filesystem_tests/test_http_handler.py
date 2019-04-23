import unittest

import chainerio
import io
import urllib.request


class TestHttpHandler(unittest.TestCase):

    def setUp(self):
        self.fs = "http"
        # TODO(kuenishi): replace this with mock to prevent web access in test
        self.text_url = "https://www.preferred-networks.jp/"
        self.non_exist_url = "https://does_not_exist.not_exist"

    def test_open_string(self):

        with urllib.request.urlopen(self.text_url) as http_data:
            with chainerio.create_handler(self.fs) as handler:
                loaded_url = handler.open(self.text_url, mode="r")
                self.assertEqual(http_data.read(), loaded_url.read())

        with urllib.request.urlopen(self.text_url) as http_data:
            with chainerio.create_handler(self.fs) as handler:
                loaded_url = handler.open(self.text_url, mode="r")
                self.assertEqual(http_data.readline(), loaded_url.readline())

    def test_open_non_exist(self):

        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(IOError, handler.open, self.non_exist_url)

    def test_list(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation, handler.list)

    def test_info(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertIsInstance(handler.info(), str)

    def test_isdir(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation, handler.isdir, "dummy")

    def test_mkdir(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation, handler.mkdir, "test")

    def test_makedirs(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.makedirs, "test/test")

    def test_exists(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.exists, "test/test")

    def test_stat(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.stat, "test/test")

    def test_rename(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.rename, "test/test", "foobar")

    def test_remove(self):
        with chainerio.create_handler(self.fs) as handler:
            self.assertRaises(io.UnsupportedOperation,
                              handler.remove, "test/test", False)
