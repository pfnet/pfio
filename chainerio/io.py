import abc
from abc import abstractmethod
from importlib import import_module

from chainerio.profiler import IOProfiler

from typing import Type, Optional, Callable, Iterator, Any
from types import TracebackType


def open_wrapper(func):
    def wrapper(self, file_path: str, mode: str = 'rb',
                buffering: int = -1, encoding: Optional[str] = None,
                errors: Optional[str] = None, newline: Optional[str] = None,
                closefd: bool = True,
                opener: Optional[Callable[
                    [str, int], Any]] = None) -> Type['IOBase']:
        file_obj = func(self, file_path, mode, buffering, encoding,
                        errors, newline, closefd, opener)
        return self._fileobject_returner(
            file_obj, file_path, mode, buffering, encoding,
            errors, newline, closefd, opener)
    return wrapper


class IO(abc.ABC):
    def __init__(self, io_profiler: Optional[IOProfiler] = None,
                 root: str = ""):
        self.io_profiler = io_profiler
        self.type = "BASEIO"
        self.root = root

    def _fileobject_returner(self, file_obj: Type['IOBase'],
                             file_path: str, mode: str = 'rb',
                             buffering: int = -1,
                             encoding: Optional[str] = None,
                             errors: Optional[str] = None,
                             newline: Optional[str] = None,
                             closefd: bool = True,
                             opener: Optional[Callable[
                                 [str, int], Any]] = None) -> Type['IOBase']:
        ''' Replace the file object from the underly system

        This function is called by the open wrapper to check and
        replace the file object returned by underly system.
        Derived class overrides this function in order to
        add functionalities when needed or match the behaviour.
        In the default case, just return the given file object.
        '''

        return file_obj

    @abstractmethod
    def open(self, file_path: str, mode: str = 'rb',
             buffering: int = -1, encoding: Optional[str] = None,
             errors: Optional[str] = None,
             newline: Optional[str] = None,
             closefd: bool = True,
             opener: Optional[Callable[
                 [str, int], Any]] = None) -> Type["IOBase"]:
        raise NotImplementedError()

    @property
    def root(self) -> str:
        return self._root

    @root.setter
    def root(self, root: str) -> None:
        self._root = root

    @abstractmethod
    def info(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def list(self, path_or_prefix: Optional[str] = None) -> Iterator:
        raise NotImplementedError()

    @abstractmethod
    def stat(self, path: str) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError()

    # TODO(tianqi) need to be changed to annotaion when we bump the
    # Python version to >=3.7
    @abstractmethod
    def __enter__(self) -> 'IO':
        raise NotImplementedError()

    @abstractmethod
    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def isdir(self, file_path: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def mkdir(self, file_path: str, mode: int = 0o777,
              *args, dir_fd: Optional[int] = None) -> None:
        raise NotImplementedError()

    @abstractmethod
    def makedirs(self, file_path: str, mode: int = 0o777,
                 exist_ok: bool = False) -> None:
        raise NotImplementedError()

    @abstractmethod
    def exists(self, file_path: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def rename(self, src: str, dst: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove(self, file_path: str, recursive: bool = False) -> None:
        '''
        Remove the file pointed by the file_path

        Args:
            file_path (str): file path to be removed.
                          It can be a file or a directory.
            recursive (boolean): When set, the remove deletes all the
                          files and directories under the given file_path,
                          includes the given file_path itself.
        '''
        raise NotImplementedError()

    # TODO(tianqi) need to be changed to annotaion when we bump the
    # Python version to >=3.7
    def _get_container_handler(self, path: str) -> Type['IO']:
        # TODO(tianqi): add detection from path
        # dynamically load module
        from chainerio.containers.zip import ZipContainer
        return ZipContainer

    # TODO(tianqi) need to be changed to annotaion when we bump the
    # Python version to >=3.7
    def open_as_container(self, container_file: str) -> 'IO':
        container_class = self._get_container_handler(container_file)
        return container_class(self, container_file)


# TODO(tianqi) need to be changed to annotaion when we bump the
# Python version to >=3.7
def create_fs_handler(fs_type: str, root: str = "") -> 'IO':
    # import for dynamic loading
    import chainerio.filesystems  # noqa

    if "" == fs_type:
        fs_type = 'posix'

    fs_module = import_module(
        ".{}".format(fs_type.lower()), 'chainerio.filesystems')
    fs_handler = getattr(fs_module, '{}FileSystem'.format(
        _format_plugin_name(fs_type)))

    handler = fs_handler(root=root)

    return handler


def _format_plugin_name(name: str) -> str:
    return name.lower().title()
