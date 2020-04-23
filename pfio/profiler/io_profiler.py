import logging
import os
import time

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class IOProfiler(object):
    def __init__(self,
                 log_base_path="", profiling=False):
        self.KB = 1000
        self.MB = self.KB*1000

        self.read_time = 0
        self.start_time = None
        self.end_time = None
        self.pid = None
        self.read_size = 0
        self.log_file_handler = None
        self.log_base_path = log_base_path
        self.profiling = profiling

        if self.profiling\
                and not os.path.exists(self.log_base_path):
            self.log_base_path = "/tmp/"
            logger.info("profile I/O")
        else:
            logger.info("do not profile I/O")

    def start_record(self, mode="READ"):
        if self.profiling:
            self.start_time = time.time()

    def end_record(self, mode="READ"):
        if self.profiling:
            self.end_time = time.time()

    def show_record(self, size=0):
        if self.profiling:
            if self.log_file_handler is None:
                self.pid = os.getpid()
            self.log_file_handler = open(os.path.join(
                self.log_base_path + str(self.pid)), 'w')
            spent_time = self.end_time - self.start_time
            self.read_time += spent_time
            self.read_size += size
            self.log_file_handler.write(
                "{}, time {} s, total time {} s throughput {} MB/s\n".format(
                    self.start_time, spent_time, self.read_time,
                    (self.read_size/self.read_time)/self.MB))
