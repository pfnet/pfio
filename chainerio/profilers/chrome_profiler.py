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
        self.init_time = time.time()
        self.ms = 1000000

    def _get_timestamp(self, time: float) -> int:
        return int((time - self.init_time) * self.ms)

    def generate_profile_dict(self, ts: float = -1,
                              event_type: str = "X") -> dict:
        if _context.context.profiling:
            if -1 == ts:
                ts = self._get_timestamp(self.start_time)
            self.matrix_dict["ts"] = ts
            self.matrix_dict["pid"] = os.getpid()
            self.matrix_dict["tid"] = threading.get_ident()
            self.matrix_dict["ph"] = event_type
            if "X" == event_type:
                self.matrix_dict["dur"] = \
                    (self.end_time - self.start_time) * self.ms
            return self.matrix_dict
        else:
            return dict()
