from chainerio.profilers.chrome_profile_writer import ChromeProfileWriter
from chainerio.profile_writer import ProfileWriter
from chainerio.profilers.naive_profiler import NaiveProfiler
from chainerio import _context

from typing import Optional, Type
import time
import os
import threading


class ChromeProfiler(NaiveProfiler):
    def __init__(self, profile_writer: Optional[Type[ProfileWriter]] = None):
        if None is profile_writer:
            profile_writer = ChromeProfileWriter()

        NaiveProfiler.__init__(self, profile_writer)

        self.name = "ChromeProfiler"

    def start_recording(self) -> None:
        if _context.context.profiling:
            self.start_time = time.time()
            self.save_profile()

    def stop_recording(self) -> None:
        if _context.context.profiling:
            self.end_time = time.time()
            self.save_profile()

    def generate_profile_dict(self, ts=0) -> dict:
        if _context.context.profiling:
            if 0 == ts:
                ts = self.start_time
            self.matrix_dict["ts"] = ts
            self.matrix_dict["pid"] = os.getpid()
            self.matrix_dict["tid"] = threading.get_ident()
            return self.matrix_dict
        else:
            return dict()
