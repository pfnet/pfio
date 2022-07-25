import abc
import contextlib
import copy
import os
import stat
from abc import abstractmethod
from io import IOBase
from types import TracebackType
from typing import Any, Callable, Iterator, Optional, Type
from urllib.parse import urlparse

from deprecation import deprecated

from pfio.version import __version__  # NOQA


class FileStat(abc.ABC):
    """Detailed file or directory information abstraction

    :meth:`pfio.v2.FS.stat` returns an object that implements of
    ``FileStat``.  In addition to the common attributes that the
    ``FileStat`` abstract provides, each ``FileStat`` subclass
    implements some additional attributes depending on what
    information the corresponding filesystem or container can handle.
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

        Note that some systems do not support directory tree semantics.

        Returns:
            `True` if directory, `False` otherwise.
        """
        return bool(self.mode & 0o40000)

    def __str__(self):
        if isinstance(self.mode, int):
            mode = stat.filemode(self.mode)
        else:
            mode = self.mode
        return '<{} filename="{}" mode="{}">'.format(
            type(self).__name__, self.filename, mode)

    def __repr__(self):
        return str(self.__str__())


class ForkedError(RuntimeError):
    '''An error class when PFIO found the process forked.

    If an FS object is not "lazy", any object usage detects process
    fork and raises this ``ForkedError`` as soon as possible at the
    child process. The parent process may or may not run well,
    depending on the ``FS`` implementation.

    '''
    pass


class FS(abc.ABC):
    '''FS access abstraction

    '''

    _cwd = ''

    def __init__(self, reset_on_fork=False):
        self.pid = os.getpid()
        self.reset_on_fork = reset_on_fork

    @property
    def cwd(self):
        return self._cwd

    @cwd.setter
    def cwd(self, value):
        self._cwd = value

    @abstractmethod
    def open(self, file_path: str, mode: str = 'rb',
             buffering: int = -1, encoding: Optional[str] = None,
             errors: Optional[str] = None,
             newline: Optional[str] = None,
             closefd: bool = True,
             opener: Optional[Callable[
                 [str, int], Any]] = None) -> Type["IOBase"]:
        raise NotImplementedError()

    def open_zip(self, file_path: str, mode='r', **kwargs) -> Type["Zip"]:  # NOQA
        from .zip import Zip
        return Zip(self, file_path, mode, **kwargs)

    def subfs(self, rel_path: str) -> Type["FS"]:
        '''Virtually changes the working directory

        By default it performs shallow copy. If any resource that as
        different lifecycles than the copy source (e.g. HDFS
        connection and zipfile.ZipFile object), they also will be
        copied by overriding this method.

        '''
        if rel_path.startswith("/"):
            raise RuntimeError("Absolute path is not supported")
        elif '..' in rel_path.split(os.path.sep):
            raise RuntimeError("Only subtree is supported")

        sub = copy.copy(self)

        sub._cwd = os.path.join(self.cwd, rel_path)
        sub._reset()
        return sub

    def _checkfork(self):
        if not self.is_forked:
            return

        # Forked!
        if self.reset_on_fork:
            self._reset()
            self.pid = os.getpid()
        else:
            raise ForkedError()

    @abstractmethod
    def _reset(self):
        raise NotImplementedError()

    @property
    def is_forked(self):
        assert hasattr(self, 'pid')
        return self.pid != os.getpid()

    def close(self) -> None:
        pass

    @abstractmethod
    def list(self, path_or_prefix: Optional[str] = None,
             recursive=False) -> Iterator[str]:
        """Lists all the files and directories under
           the given ``path_or_prefix``

        Args:
            path_or_prefix (str): The path to list against.
                When we get the default value, ``list`` shows the content under
                the working directory as the default value.
                However, if a ``path_or_prefix`` is given,
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

    def __enter__(self) -> 'FS':
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.close()

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
    def exists(self, path: str) -> bool:
        """Returns the existence of the path

        When the ``file_path`` points to a symlink, the return value
        depends on the actual file instead of the link itself.

        """
        raise NotImplementedError()

    @abstractmethod
    def rename(self, src: str, dst: str) -> None:
        """Renames the file from ``src`` to ``dst``

        On systems and situation where rename functionality is
        proviced, it renames the file or the directory.


        Args:
            src (str): the current name of the file or directory.

            dst (str): the name to rename to.

        """
        raise NotImplementedError()

    @abstractmethod
    def remove(self, file_path: str, recursive: bool = False) -> None:
        """Removes a file or directory

           Args:
               path (str): the target path to remove. The ``path`` can be a
               regular file or a directory.

               recursive (bool): When the given path is a directory,
                   all the files and directories under it will be removed.
                   When the path is a file, this option is ignored.

        """
        raise NotImplementedError()


@contextlib.contextmanager
def open_url(url: str, mode: str = 'r', **kwargs) -> 'IOBase':
    '''Opens a file regardless of the backend FS type

    ``url`` must be compliant with URL standard in
    https://url.spec.whatwg.org/ .  As this function implements
    context manager, the FileObject can be written as::

       with open_url("s3://bucket.example.com/path/your-file.txt", 'r') as f:
           f.read()

    .. note:: Some FS resouces won't be closed when using this
        functionality. See ``from_url`` for keyword arguments.

    Returns:
        a FileObject that must be closed.

    '''
    dirname, filename = os.path.split(url)
    with from_url(dirname, **kwargs) as fs:
        with fs.open(filename, mode) as fp:
            yield fp


def from_url(url: str, **kwargs) -> 'FS':
    '''Factory pattern implementation, creates FS from URI

    If ``force_type`` is set with archive type, not scheme,
    it ignores the suffix and tries the specified archive
    format by opening the blob file.

    If ``force_type`` is set with scheme type, the FS will
    built from it accordingly. The URL path is supposed to
    be a directory for file systems or a path prefix for S3.

    .. warning:: When opening an ``hdfs://...`` URL, be sure about
        forking context. See: :class:`Hdfs` for discussion.

    Arguments:
        url (str): A URL string compliant with RFC 1738.

        force_type (str): Force type of FS to be returned.
            One of "zip", "hdfs", "s3", or "file", returned
            respectively. Default is ``"file"``.

        create (bool): Create the specified path doesn't exist.

        reset_on_fork (bool): Reset stateful objects (e.g. connection
            to the remote system) after fork.

    .. note:: Some FS resouces won't be closed when using this
        functionality.

    .. note:: Pickling the FS object may or may not work correctly
        depending on the implementation.

    '''
    parsed = urlparse(url)
    force_type = kwargs.pop('force_type', None)

    if parsed.scheme:
        scheme = parsed.scheme
    else:
        scheme = 'file'  # Default is local

    # When ``force_type`` is defined, it must be equal with given one.
    if force_type is not None and force_type != "zip":
        if force_type != scheme:
            raise ValueError("URL scheme mismatch with forced type")

    def _zip_check_create_not_supported():
        if kwargs.get('create', False):
            msg = '"create" option is not supported for Zip FS.'
            raise ValueError(msg)

    # force_type \ suffix | .zip    | other
    # --------------------+---------+------
    #                 zip | ok      | try zip
    #             (other) | try dir | try dir
    #                None | try zip | try dir
    if force_type == 'zip':
        _zip_check_create_not_supported()
        dirname, filename = os.path.split(parsed.path)
        fs = _from_scheme(scheme, dirname, kwargs, bucket=parsed.netloc)
        fs = fs.open_zip(filename, **kwargs)

    elif force_type is None:
        if parsed.path.endswith('.zip'):
            _zip_check_create_not_supported()
            dirname, filename = os.path.split(parsed.path)
            fs = _from_scheme(scheme, dirname, kwargs, bucket=parsed.netloc)
            fs = fs.open_zip(filename, **kwargs)
        else:
            dirname = parsed.path
            fs = _from_scheme(scheme, dirname, kwargs, bucket=parsed.netloc)

    else:
        dirname = parsed.path
        fs = _from_scheme(scheme, dirname, kwargs, bucket=parsed.netloc)

    return fs


def _from_scheme(scheme, dirname, kwargs, bucket=None):
    if scheme == 'file':
        from .local import Local
        fs = Local(dirname, **kwargs)
    elif scheme == 'hdfs':
        from .hdfs import Hdfs
        fs = Hdfs(dirname, **kwargs)
    elif scheme == 's3':
        from .s3 import S3
        fs = S3(bucket=bucket, prefix=dirname, **kwargs)
    else:
        raise RuntimeError("Scheme {} is not supported", scheme)

    return fs


@deprecated(deprecated_in='2.2.0', removed_in='2.3.0',
            current_version=__version__)
def lazify(init_func, lazy_init=True, recreate_on_fork=True) -> "FS":
    '''Make FS init lazy and recreate on fork

    '''
    return _LazyFS(init_func, lazy_init, recreate_on_fork)


class _LazyFS:
    def __init__(self, func, lazy_init, recreate_on_fork):
        if not lazy_init:
            self.mixin = func()
        else:
            self.mixin = None
        self.func = func

    def __getattr__(self, attr):
        if self.mixin is None or self.mixin.is_forked:
            self.mixin = self.func()
        return getattr(self.mixin, attr)

    def __enter__(self) -> 'FS':
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.close()
