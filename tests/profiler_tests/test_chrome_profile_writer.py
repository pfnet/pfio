import unittest

from chainerio.profilers.chrome_profile_writer import ChromeProfileWriter
import chainerio
import json


class TestChromeProfileWriter(unittest.TestCase):

    def setUp(self):
        self.test_dict = {"key1": "value1", "key2": "value2"}

    def test_write_profile(self):
        profilewriter = ChromeProfileWriter()
        profilewriter.dump_profile(self.test_dict)

        profile_path = profilewriter.profile_file_path

        additional_keys = ["displayTimeUnit", "systemTraceEvents", "otherData"]

        with chainerio.open(profile_path, "r") as f:
            data = json.load(f)
            self.assertEqual(self.test_dict, data["traceEvents"])

            print(data)

            for keys in additional_keys:
                self.assertIn(keys, data)

        chainerio.remove(profile_path)
