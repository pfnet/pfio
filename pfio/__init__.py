from io import IOBase
from typing import Any, Callable, Iterator, Type

import pfio.cache  # NOQA
from pfio._context import DefaultContext
from pfio._typing import Optional, Union
from pfio.io import IO
from pfio.version import __version__  # NOQA

_DEFAULT_CONTEXT = DefaultContext()


def open_as_container(path: str) -> IO:
    """Opens a container and returns the handler

       Call the corresponding :func:`IO.open_as_container` upon the
       default handler.

       The ``path`` can be a POSIX path or a URI.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    return default_context.open_as_container(path)


def list(path_or_prefix: Optional[str] = None,
         recursive: bool = False) -> Iterator[str]:
    """Lists all the files and directories under the given ``path_or_prefix``

       Call the corresponding :func:`IO.list` upon the default handler.

       The ``path`` can be a POSIX path or a URI.

    """

    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    if None is path_or_prefix:
        path_or_prefix = ""

    (handler, actual_path) = default_context.get_handler(path_or_prefix)
    return handler.list(actual_path, recursive)


def info() -> str:
    """Shows the detail of the current default handler

       Call the corresponding :func:`IO.info` upon the default handler.

    Returns:
        A string that describes the details of the default handler.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, dummy_path) = default_context.get_handler()
    return handler.info()


def open(file_path: str, mode: str = 'rb',
         buffering: int = -1, encoding: Optional[str] = None,
         errors: Optional[str] = None, newline: Optional[str] = None,
         closefd: bool = True,
         opener: Optional[Callable[
             [str, int], Any]] = None) -> Type['IOBase']:
    """Opens a regular file with ``mode``

    Call the corresponding :func:`IO.open` upon the default handler.

    If a POSIX path is given, the method use the default handler,
    and identifies the file from root path.
    See :func:`set_root` for details about root path and default handler.
    If a URI is given, right filesystem handler is automatically chosen
    by the library, according to the scheme included in the URI.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path) = default_context.get_handler(file_path)
    return handler.open(
        actual_path, mode, buffering, encoding,
        errors, newline, closefd, opener)


def set_root(uri_or_handler: Union[str, Type['IO']]) -> None:
    """Sets the current context to ``uri_or_handler``

    The context here refers to the default handler and the root path.
    The default handler points to a filesystem or a container which
    PFIO uses when called without a URI.
    Handlers can be created by :func:`create_handler` with
    the name of scheme. See the case 3 in the following example.

    Example::

        # Case 1
        # set_root by the name of uri
        pfio.set_root("posix")
        # open a file on posix filesystem with path "some/file"
        pfio.open("some/file")
        # override with a uri
        pfio.open("hdfs:///some/file/on/hdfs")

        # Case 2
        # set_root by uri
        pfio.set_root("hdfs:///some/directory")
        # open a file "on/hdfs" on hdfs under the " some/directory"
        pfio.open("on/hdfs")

        # Case 3
        handler = pfio.create_handler('hdfs')
        # set_root by handler
        pfio.set_root(handler)
        # open a file "/some/file/on/hdfs" on hdfs
        pfio.open("/some/file/on/hdfs")

        # Case 4
        handler = pfio.open_as_container('some.zip')
        # set_root by handler
        pfio.set_root(handler)
        # open a file "img.jpg" in "some.zip"
        pfio.open("img.jpg")

    The root path refers to a directory that PFIO works on.
    It is similar to current working directory, ``CWD`` in terms of the shell
    environment.
    The root path will only be set
    when the ``uri_or_handler`` points to a directory.
    Otherwise, it will be set to default,
    which represents the default working directory as follows:

    +---------+---------------------------+
    |         | Default Working Directory |
    +=========+===========================+
    | POSIX   | current working directory |
    +---------+---------------------------+
    | HDFS    | /user/``USERNAME`` [#f1]_ |
    +---------+---------------------------+
    | zip     |      top directory        |
    +---------+---------------------------+

    .. [#f1] For the details about the ``USERNAME`` in HDFS, please refer to \
    `HDFS Document <https://hadoop.apache.org/docs/current/\
            hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html>`_


    Args:
        uri_or_handler (str or a object of a derived class of
            :class:`pfio.IO`): The ``uri_or_handler``
            can accept the following three kinds of values:

            1. the scheme (string): set the default handler
                according to the scheme.
                See :func:`create_handler` for supported scheme.

            2. a uri of directory (string): set the context
                to use the corresponding handler and set
                the root path to the given directory.

            3. a handler, which is an instance of
                :class:`pfio.IO`. Set the default handler
                to the given handler, and root path to default.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    default_context.set_root(uri_or_handler)


def create_handler(scheme: str) -> IO:
    """Returns a handler according to the given ``scheme``

    The current supported handlers are:

    1. posix
    2. hdfs

    See `scheme <https://github.com/pfnet/pfio/blob/master/\
            docs/source/design.rst#uri-expression-of-file-paths>`_
    for more details.


    Args:
        scheme (str): the name of the scheme.

    Returns:
        An object that implements the APIs defined in :class:`pfio.IO`.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    if not default_context.is_supported_scheme(scheme):
        raise ValueError("scheme {} is not supported".format(scheme))

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(scheme)
    return handler


def isdir(path: str) -> bool:
    """Returns ``True`` if the path is an existing directory

    Call the corresponding :func:`IO.isdir` upon the default handler.

    The ``path`` can be a POSIX path or a URI.
    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.isdir(actual_path)


def mkdir(path: str, mode: int = 0o777, *,
          dir_fd: Optional[int] = None) -> None:
    """Makes a directory with mode

    Call the corresponding :func:`IO.mkdir` upon the default handler.

    The ``path`` can be a POSIX path or a URI.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.mkdir(actual_path, mode, dir_fd=dir_fd)


def makedirs(path: str, mode: int = 0o777,
             exist_ok: bool = False) -> None:
    """Makes directories recursively with mode

    Call the corresponding :func:`IO.makedirs` upon the default handler.

    The ``path`` can be a POSIX path or a URI.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.makedirs(actual_path, mode, exist_ok)


def exists(path: str) -> bool:
    """Returns ``True`` when the given ``path`` exists

    Call the corresponding :func:`IO.exists` upon the default handler.

    The ``path`` can be a POSIX path or a URI. or URIs.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.exists(actual_path)


def rename(src: str, dst: str) -> None:
    """Renames the file from ``src`` to ``dst``

    Call the corresponding :func:`IO.rename` upon the default handler.

    Note the ``src`` and ``dst`` SHOULD be in the same filesystem.
    The ``src`` and ``dst`` can be either POSIX paths or URIs.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler_src, actual_src, _is_URI1) = \
        default_context.get_handler_by_name(src)
    (handler_dst, actual_dst, _is_URI2) = \
        default_context.get_handler_by_name(dst)
    # TODO: containers are not supported here
    if type(handler_src) != type(handler_dst):
        raise NotImplementedError(
            "Moving between different systems is not supported")
    handler_src.rename(actual_src, actual_dst)


def remove(path: str, recursive: bool = False) -> None:
    """Removes a file or directory

    Call the corresponding :func:`IO.remove` upon the default handler.

    Args:
        path (str): the target path to remove. The ``path`` can be a
        regular file or a directory.
        The ``path`` can be a POSIX file path or a URI.

        recursive (bool): When the given path is a directory,
            all the files and directories under it will be removed.
            When the path is a file, this option is ignored.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.remove(actual_path, recursive)


def get_root_dir() -> str:
    """get the current root path

    Returns:
        The current root path

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    return default_context.get_root_dir()
