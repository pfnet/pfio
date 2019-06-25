import unittest

import chainerio


class TestProfiler(unittest.TestCase):

    def setUp(self):
        self.test_dict = {"key1": "value1", "key2": "value2"}
        self.dummy_loop = 100
        self.test_file = "testfile.txt"
        self.test_content = "this is a test content"

    def dummy_fileio(self):
        with chainerio.open(self.test_file, "w") as f:
            f.write(self.test_content)
        chainerio.remove(self.test_file)

    def test_decorator(self):
        profiler = chainerio.context.profiler
        profiler.clean()

        self.dummy_fileio()
        print(profiler.show())
        self.assertEqual(len(profiler.show()), 0)

        with chainerio.profiling():
            self.dummy_fileio()
            self.assertNotEqual(len(profiler.show()), 0)

        self.dummy_fileio()
        self.assertNotEqual(len(profiler.show()), 0)

    def test_func_profiling(self):
        profiler = chainerio.context.profiler
        profiler.clean()

        with chainerio.profiling():
            with chainerio.open(self.test_file, "w") as f:
                f.write(self.test_content)

            self.assertNotEqual(dict(), profiler.show())

        chainerio.remove(self.test_file)
