import abc
from abc import abstractmethod

from typing import Union, Optional


class ProfileWriter(abc.ABC):
    def __init__(self, profile_file_path: str = None):
        self.name = "BaseProfileWriter"
        self.profile_file_path = profile_file_path
        self.file_obj = None

    @property
    def profile_file_path(self) -> Union[None, str]:
        return self._profile_file_path

    @profile_file_path.setter
    def profile_file_path(self, profile_file_path: str) -> None:
        self._profile_file_path = profile_file_path

    @abstractmethod
    def dump_profile(self, obj: dict, file_path: Optional[str] = None) -> None:
        raise NotImplementedError()
