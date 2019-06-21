import unittest

from chainerio.profilers import ChromeProfiler
import chainerio


class TestChromeProfiler(unittest.TestCase):

    def setUp(self):
        self.test_dict = {"key1": "value1", "key2": "value2"}
        self.dummy_loop = 100
        self.test_file = "testfile.txt"
        self.test_content = "this is a test content"

    def dummy_cal(self):
        _sum = 0
        for i in range(self.dummy_loop):
            _sum += i
        return _sum

    def dummy_fileio(self):
        with chainerio.open(self.test_file, "w") as f:
            f.write(self.test_content)
        chainerio.remove(self.test_file)

    def test_profiler(self):
        profiler = ChromeProfiler()

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

                profiler.dump()
                profiler_path = profiler.get_profile_file_path()

                self.assertTrue(chainerio.exists(profiler_path))
                chainerio.remove(profiler_path)

        profiler.reset()
        with profiler:
            self.dummy_cal()
            time = profiler.recorded_time
            self.assertEqual(time, 0.0)

    def test_matrix(self):
        profiler = ChromeProfiler()
        additional_keys = ["ts", "pid", "tid"]

        with chainerio.profiling():
            for key, value in self.test_dict.items():
                profiler.add_matrix(key, value)

            for key in self.test_dict.keys():
                self.assertIn(key, profiler.generate_profile_dict())

            for key in additional_keys:
                self.assertIn(key, profiler.generate_profile_dict())

            profiler.reset()
