import unittest

import chainerio


class TestSimpleIO(unittest.TestCase):

    def setUp(self):
        self.test_filename = "testfile"
        self.test_filename2 = "testfile2"
        self.content = "this is a test content"

    def tearDown(self):
        profiler = chainerio.context.profiler
        print(profiler.get_profile_file_path())
        chainerio.remove(profiler.get_profile_file_path())

    def test_simple_io(self):
        with chainerio.profiling():
            with chainerio.open(self.test_filename, "w") as f:
                f.write(self.content)
            chainerio.remove(self.test_filename)
        with chainerio.profiling():
            with chainerio.open(self.test_filename2, "w") as f:
                f.write(self.content)
            chainerio.remove(self.test_filename2)

            chainerio.dump_profile()
