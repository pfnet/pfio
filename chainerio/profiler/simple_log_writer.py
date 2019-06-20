from chainerio.profiler import LogWriter
import os
from io import IOBase
import chainerio

DEFAULT_LOG_PATH = os.path.join(
    os.getenv('HOME'), ".chainer", "chainerio", "log")


class SimpleLogWriter(LogWriter):
    def __init__(self, log_dir: str = DEFAULT_LOG_PATH,
                 log_filename: str = None):
        self.name = "SimpleLogWriter"

        self.log_dir = log_dir

        if None is log_filename:
            self.log_filename = "log.{}".format(os.getpid())
        else:
            self.log_filename = log_filename

        log_file_path = os.path.join(self.log_dir, self.log_filename)
        LogWriter.__init__(self, log_file_path)

    def _open_file_obj(self) -> IOBase:
        if None is self.file_obj:
            if not os.path.exists(self.log_dir):
                os.path.makedirs(self.log_dir)

            self.file_obj = open(self.log_file_path, "w")

    def write_log(self, obj: dict, sync: bool = False) -> None:
        self._open_file_obj()

        self.file_obj.write(str(obj))
        if sync:
            self.file_obj.flush()

    # def __exit__(self, *exc):
    #     if None is not self.file_obj:
    #         self.file_obj.close()

    # def __enter__(self):
    #     return self
