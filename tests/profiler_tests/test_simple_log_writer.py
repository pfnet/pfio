import unittest

from chainerio.profiler import SimpleLogWriter
import chainerio


class TestSimpleLogWriter(unittest.TestCase):

    def setUp(self):
        self.test_dict = {"key1": "value1", "key2": "value2"}

    def test_write_log(self):
        logwriter = SimpleLogWriter()
        logwriter.write_log(self.test_dict, True)

        log_path = logwriter.log_file_path

        with chainerio.open(log_path, "r") as f:
            data = f.read()
            self.assertEqual(str(self.test_dict), data)

        chainerio.remove(log_path)
