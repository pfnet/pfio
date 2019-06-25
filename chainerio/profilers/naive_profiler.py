from chainerio.profilers.naive_profile_writer import NaiveProfileWriter
from chainerio.profile_writer import ProfileWriter
from chainerio.profiler import Profiler
from chainerio import _context


from typing import Optional, Type
import time
import os
import threading


class NaiveProfiler(Profiler):
    def __init__(self, profile_writer: Optional[Type[ProfileWriter]] = None):
        if None is profile_writer:
            profile_writer = NaiveProfileWriter()

        Profiler.__init__(self, profile_writer)

        self.name = "SimpleProfiler"

    @property
    def recorded_time(self) -> float:
        if _context.context.profiling and -1 != self.start_time:
            return self.end_time - self.start_time
        else:
            return 0.0

    def start_recording(self) -> None:
        if _context.context.profiling:
            self.start_time = time.time()

    def stop_recording(self) -> None:
        if _context.context.profiling:
            self.end_time = time.time()

    def generate_profile_dict(self, ts: float = 0,
                              event_type: str = "X") -> dict:
        if _context.context.profiling:
            self.matrix_dict["ts"] = self.start_time
            self.matrix_dict["pid"] = os.getpid()
            self.matrix_dict["tid"] = threading.get_ident()
            self.matrix_dict["time"] = self.recorded_time
            self.matrix_dict["event_type"] = event_type
            return self.matrix_dict
        else:
            return dict()

    def get_profile_file_path(self) -> str:
        return self.profile_writer.profile_file_path

    def dump(self, filepath: Optional[str] = None) -> None:
        if _context.context.profiling:
            self.profile_writer.dump_profile(self.profile_list, filepath)

    def __exit__(self, type, value, traceback):
        self.stop_recording()
        self.save_profile()
        self.reset()
