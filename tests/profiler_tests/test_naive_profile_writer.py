import unittest

from chainerio.profilers.naive_profile_writer import NaiveProfileWriter
import chainerio


class TestNaiveProfileWriter(unittest.TestCase):

    def setUp(self):
        self.test_dict = {"key1": "value1", "key2": "value2"}

    def test_write_profile(self):
        profilewriter = NaiveProfileWriter()
        profilewriter.dump_profile(self.test_dict)

        profile_path = profilewriter.profile_file_path

        with chainerio.open(profile_path, "r") as f:
            data = f.read()
            self.assertEqual(str(self.test_dict), data)

        chainerio.remove(profile_path)
