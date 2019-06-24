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
            self.save_profile(
                self.generate_profile_dict(ts=self.start_time,
                                           event_type="B"))

    def stop_recording(self) -> None:
        if _context.context.profiling:
            self.end_time = time.time()
            self.save_profile(
                self.generate_profile_dict(ts=self.end_time,
                                           event_type="E"))

    def generate_profile_dict(self, ts: float = 0,
                              event_type: str = "X") -> dict:
        if _context.context.profiling:
            if 0 == ts:
                ts = self.start_time
            self.matrix_dict["ts"] = ts
            self.matrix_dict["pid"] = os.getpid()
            self.matrix_dict["tid"] = threading.get_ident()
            self.matrix_dict["ph"] = event_type
            if "X" == event_type:
                self.matrix_dict["dur"] = \
                    (self.end_time - self.start_time) * 1000
            return self.matrix_dict
        else:
            return dict()

    def __exit__(self, type, value, traceback):
        self.stop_recording()
        self.reset()
