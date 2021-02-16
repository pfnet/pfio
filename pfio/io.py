import abc
import stat
from abc import abstractmethod
from importlib import import_module
from io import IOBase
from types import TracebackType
from typing import Any, Callable, Iterator, Type

from pfio._typing import Optional
from pfio.profiler import IOProfiler


def open_wrapper(func):
    def wrapper(self, file_path: str, mode: str = 'rb',
                buffering: int = -1, encoding: Optional[str] = None,
                errors: Optional[str] = None, newline: Optional[str] = None,
                closefd: bool = True,
                opener: Optional[Callable[
                    [str, int], Any]] = None) -> Type['IOBase']:
        file_obj = func(self, file_path, mode, buffering, encoding,
                        errors, newline, closefd, opener)
        return self._wrap_fileobject(
            file_obj, file_path, mode, buffering, encoding,
            errors, newline, closefd, opener)
    return wrapper


class FileStat(abc.ABC):
    """Detailed file or directory information abstraction

    :meth:`pfio.IO.stat` of filesystem/container handlers return an object of
    subclass of ``FileStat``.
    In addition to the common attributes that the ``FileStat`` abstract
    provides, each ``FileStat`` subclass implements some additional
    attributes depending on what information the corresponding filesystem or
    container can handle.
    The common attributes have the same behavior despite filesystem or
    container type difference.

    Attributes:
        filename (str):
            Filename in the filesystem or container.
        last_modifled (float):
            UNIX timestamp of mtime. Note that some
            filesystems or containers do not have sub-second precision.
        mode (int):
            Permission with file type flag (regular file or directory).
            You can make a human-readable interpretation by
            `stat.filemode <https://docs.python.org/3/library/stat.html#stat.filemode>`_.
        size (int):
            Size in bytes. Note that directories may have different
            sizes depending on the filesystem or container type.
    """     # NOQA
    filename = None
    last_modified = None
    mode = None
    size = None

    def isdir(self):
        """Returns whether the target is a directory, based on the permission flag

        Returns:
            `True` if directory, `False` otherwise.
        """
        return bool(self.mode & 0o40000)

    def __str__(self):
        return '<{} filename="{}" mode="{}">'.format(
            type(self).__name__, self.filename, stat.filemode(self.mode))

    def __repr__(self):
        return str(self.__str__())


class IO(abc.ABC):
    def __init__(self, io_profiler: Optional[IOProfiler] = None,
                 root: str = ""):
        self.io_profiler = io_profiler
        self.type = "BASEIO"
        self.root = root

    def _wrap_fileobject(self, file_obj: Type['IOBase'],
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
        In the default case, it just returns the given file object.
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
        """Opens a regular file with ``mode``

        The function returns a file object, and the type depends on
        the filesystem of the file and ``mode``.

        Args:
            file_path (str): the target file path

            mode (str): the open mode of the file. Currently, the
            following modes are supported:

            +----+-----------------------+
            |mode|      Meaning          |
            +====+=======================+
            | r  | read as a text file   |
            +----+-----------------------+
            | w  | write as a text file  |
            +----+-----------------------+
            | rb | read as a binary file |
            +----+-----------------------+
            | wb | write as a binary file|
            +----+-----------------------+

        Returns:
            A file object according to the ``mode``.

        """
        raise NotImplementedError()

    @property
    def root(self) -> str:
        return self._root

    @root.setter
    def root(self, root: str) -> None:
        self._root = root

    @abstractmethod
    def info(self) -> str:
        """Shows the detail of the current handler

        Returns:
        A string that describes the details of the default handler.

        """
        raise NotImplementedError()

    @abstractmethod
    def list(self, path_or_prefix: Optional[str] = None,
             recursive=False) -> Iterator[str]:
        """Lists all the files and directories under
           the given ``path_or_prefix``

        Args:
            path_or_prefix (str): The path to list against.
                When we get the default value, ``list`` shows the content under
                the root path, as the default value.
                Refer to :func:`set_root` for details about the root path of
                each filesystem. However, if a ``path_or_prefix`` is given,
                then it shows only the files and directories
                under the ``path_or_prefix``.

            recursive (bool): When this is ``True``, list files and directories
                recursively.

        Returns:
            An Iterator that iterates though the files and directories.

        """
        raise NotImplementedError()

    @abstractmethod
    def stat(self, path: str) -> FileStat:
        """Show details of a file

        It returns an object of subclass of :class:`pfio.io.FileStat`
        in accordance with filesystem or container type.

        Args:
            path (str): The path to file

        Returns:
            :class:`pfio.io.FileStat` object.
        """
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
        """Returns ``True`` if the path is an existing directory

        Args:
            path (str): the path to the target directory

        Returns:
            ``True`` when the path points to a directory,
            ``False`` when it is not

        """
        raise NotImplementedError()

    @abstractmethod
    def mkdir(self, file_path: str, mode: int = 0o777,
              *args, dir_fd: Optional[int] = None) -> None:
        """Makes a directory with mode

        Args:
            path (str): the path to the directory to make

            mode (int): the mode of the new directory

        """
        raise NotImplementedError()

    @abstractmethod
    def makedirs(self, file_path: str, mode: int = 0o777,
                 exist_ok: bool = False) -> None:
        """Makes directories recursively with mode

        Also creates all the missing parents of the given path.

        Args:
            path (str): the path to the directory to make.

            mode (int): the mode of the directory

            exist_ok (bool): In default case, a ``FileExitsError`` will be
                raised when the target directory exists.

        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, file_path: str) -> bool:
        """Returns ``True`` when the given ``path`` exists

        When the ``file_path`` points to a symlink, the return value
        depends on the actual file instead of the link itself.

        Args:
            path (str): the ``path`` to the target file. The ``path`` can be a
            POSIX path or a URI.

        Returns:
            ``True`` when the file or directory exists,
            ``False`` when it is not.

        """
        raise NotImplementedError()

    @abstractmethod
    def rename(self, src: str, dst: str) -> None:
        """Renames the file from ``src`` to ``dst``

        Args:
            src (str): the current name of the file or directory.

            dst (str): the name to rename to.

        """
        raise NotImplementedError()

    @abstractmethod
    def remove(self, file_path: str, recursive: bool = False) -> None:
        """Removes a file or directory

           A combination of :func:`os.remove` and :func:`os.rmtree`.

           Args:
               path (str): the target path to remove. The ``path`` can be a
               regular file or a directory.

               recursive (bool): When the given path is a directory,
                   all the files and directories under it will be removed.
                   When the path is a file, this option is ignored.

        """
        raise NotImplementedError()

    # TODO(tianqi) need to be changed to annotaion when we bump the
    # Python version to >=3.7
    def _get_container_handler(self, path: str) -> Type['IO']:
        # TODO(tianqi): add detection from path
        # dynamically load module
        from pfio.containers.zip import ZipContainer
        return ZipContainer

    # TODO(tianqi) need to be changed to annotaion when we bump the
    # Python version to >=3.7
    def open_as_container(self, container_file: str) -> 'IO':
        """Opens a container and returns the handler

        This function opens a container, e.g. zip, instead of a regular file.
        For more details about the container, please refer to the `design \
                <https://github.com/pfnet/pfio/blob/master/docs/\
                source/design.rst#containers>`_

        Works when the current handler is also a container: nested container.

        Args:
            path (str): The path to the container.

        Returns:
            A container handler that implements methods defined in
            :class:`pfio.container.Container`, which derived from
            :class:`pfio.IO`. The type of the container is
            determined by the extension of the given path.
            Currently, only zip is supported.

        """
        container_class = self._get_container_handler(container_file)
        return container_class(self, container_file)


# TODO(tianqi) need to be changed to annotaion when we bump the
# Python version to >=3.7
def create_fs_handler(fs_type: str, root: str = "") -> 'IO':
    # import for dynamic loading
    import pfio.filesystems  # noqa

    if "" == fs_type:
        fs_type = 'posix'

    fs_module = import_module(
        ".{}".format(fs_type.lower()), 'pfio.filesystems')
    fs_handler = getattr(fs_module, '{}FileSystem'.format(
        _format_plugin_name(fs_type)))

    handler = fs_handler(root=root)

    return handler


def _format_plugin_name(name: str) -> str:
    return name.lower().title()
