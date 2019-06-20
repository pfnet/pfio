import abc
from abc import abstractmethod

from typing import Union


class LogWriter(abc.ABC):
    def __init__(self, log_file_path: str = None):
        self.name = "BaseLogWriter"
        self.log_file_path = log_file_path
        self.file_obj = None

    @property
    def log_file_path(self) -> Union[None, str]:
        return self._log_file_path

    @log_file_path.setter
    def log_file_path(self, log_file_path: str) -> None:
        self._log_file_path = log_file_path

    @abstractmethod
    def write_log(self, obj: dict) -> None:
        raise NotImplementedError()
