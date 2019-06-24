from chainerio.profile_writer import ProfileWriter
import os
from io import IOBase

from typing import Optional
import json

DEFAULT_LOG_PATH = os.path.join(
    os.getenv('HOME'), ".chainer", "chainerio", "profile")


class ChromeProfileWriter(ProfileWriter):
    def __init__(self, profile_dir: str = DEFAULT_LOG_PATH,
                 profile_filename: str = None):
        self.name = "ChromeProfileWriter"

        self.profile_dir = profile_dir

        if None is profile_filename:
            self.profile_filename = "chrome_profile.{}.json".format(
                os.getpid())
        else:
            self.profile_filename = profile_filename

        profile_file_path = os.path.join(self.profile_dir,
                                         self.profile_filename)
        ProfileWriter.__init__(self, profile_file_path)

    def _create_profile_dir(self, file_path) -> IOBase:
        _dir = os.path.dirname(file_path)
        if not os.path.exists(_dir):
            os.makedirs(_dir)

    def dump_profile(self, profile_list: dict,
                     file_path: Optional[str] = None) -> None:
        if None is file_path:
            file_path = self.profile_file_path

        self._create_profile_dir(file_path)

        trace = {"traceEvents": profile_list,
                 "displayTimeUnit": "ms",
                 "systemTraceEvents": "ChainerIOEventTrace",
                 "otherData": {"app": "ChainerIO profile"}, }

        with open(file_path, "w") as f:
            json.dump(trace, f)
