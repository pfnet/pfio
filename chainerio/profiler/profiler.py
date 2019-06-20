from chainerio.profiler import SimpleLogWriter
from chainerio.profiler import LogWriter
from chainerio import _context

from typing import Optional, Any, Union, Type
import time

import functools


class Profiler(object):
    def __init__(self, log_writer: Optional[Type[LogWriter]] = None):
        self.name = "BaseProfiler"
        if None is log_writer:
            self.log_writer = SimpleLogWriter()
        else:
            self.log_writer = log_writer

        self.log_dict = dict()
        self.start_time = -1

    @property
    def recorded_time(self) -> float:
        if _context.context._profiling and -1 != self.start_time:
            return time.time() - self.start_time
        else:
            return 0.0

    def start_recording(self) -> None:
        if _context.context._profiling:
            self.start_time = time.time()

    def reset(self):
        self.start_time = -1
        self.reset_dict()

    def reset_dict(self) -> None:
        if _context.context._profiling:
            self.log_dict = dict()

    def add_log_value(self, key: Any, value: Any) -> None:
        if _context.context._profiling:
            self.log_dict[key] = value

    def generate_log(self) -> dict:
        if _context.context._profiling:
            self.log_dict["time"] = self.recorded_time
            return self.log_dict
        else:
            return dict()

    def get_log_file_path(self) -> Union[str, None]:
        if _context.context._profiling:
            return self.log_writer.log_file_path
        else:
            return None

    def save_log(self, log_dict: Optional[dict] = None) -> None:
        if not _context.context._profiling:
            return

        if None is log_dict:
            log = self.generate_log()
        else:
            # merge the new log_dict with the old log_dict
            log = {**log_dict, **self.log_dict}

        self.log_writer.write_log(log)

    def __enter__(self):
        self.start_recording()

    def __exit__(self, typ, value, traceback):
        self.save_log()
        self.reset()


def profiler_decorator(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        profiler = args[0].io_profiler
        with profiler:
            profiler.add_log_value("func", func.__name__)
            profiler.add_log_value("args", args)
            profiler.add_log_value("kwargs", kwargs)

            ret = func(*args, **kwargs)

        return ret

    return inner


def profiling():
    return _context.using_config('_profiling', True)
