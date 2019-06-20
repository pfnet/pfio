import unittest

import chainerio.profiler
from chainerio.profiler import Profiler
import os


class TestProfiler(unittest.TestCase):

    def setUp(self):
        self.test_dict = {"key1": "value1", "key2": "value2"}
        self.dummy_loop = 100

    def dummy_cal(self):
        _sum = 0
        for i in range(self.dummy_loop):
            _sum += i
        return _sum

    def test_profiler(self):
        profiler = Profiler()

        with profiler:
            self.dummy_cal()
            time = profiler.recorded_time
            self.assertEqual(time, 0.0)
            profiler.reset()

        with chainerio.profiling():
            with profiler:
                self.dummy_cal()
                time = profiler.recorded_time
                self.assertNotEqual(time, 0.0)

        profiler.reset()
        with profiler:
            self.dummy_cal()
            time = profiler.recorded_time
            self.assertEqual(time, 0.0)

    def test_log_dict(self):
        profiler = Profiler()

        with chainerio.profiling():
            for key, value in self.test_dict.items():
                profiler.add_log_value(key, value)

            for key in self.test_dict.keys():
                self.assertIn(key, profiler.generate_log())
            profiler.reset()

    def test_func_profiling(self):
        testfile = "test.file"
        with chainerio.profiling():

            with chainerio.open(testfile, "w") as f:
                log_path = f.io_profiler.get_log_file_path()
                f.write("this is a test string")

            self.assertTrue(os.path.exists(log_path))

        chainerio.remove(testfile)
        chainerio.remove(log_path)
