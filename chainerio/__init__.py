from chainerio._context import DefaultContext
from chainerio.version import __version__  # NOQA

from chainerio.io import IO
from chainerio._typing import Optional, Union
from typing import Iterator, Any, Callable, Type


_DEFAULT_CONTEXT = DefaultContext()


def open_as_container(path: str) -> IO:
    """Opens a container files and returns the handler

    This function works similar to the :func:`open`,
    while it opens a container, e.g. zip, instead of a regular file.

    Args:
        path (str): The path to the container. The path can be 
        an Unix path or an URI.

    Returns:
        A container handler that implements methods defined in
        :class:`chainerio.Container`, which derived from
        :class:`chainerio.IO`. The type of the container is
        determined by the extension of the given path.
        Currently, only zip is supported.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    return default_context.open_as_container(path)


def list(path_or_prefix: Optional[str] = None,
         recursive: bool = False) -> Iterator[str]:
    """Lists all the files and directories under the given ``path_or_prefix``

    Args:
        path_or_prefix (str): The path to list against.
            When we get the default value, ``list`` shows the content under
            the root path, as the default value.
            Refer to :func:`set_root` for details about the root path of
            each filesystem. However, if a ``path_or_prefix`` is given,
            then it shows only the files and directories
            under the ``path_or_prefix``. The ``path_or_prefix`` can be an
            Unix path or an URI.

        recursive (bool): When this is ``True``, list files and directories
            recursively.

    Returns:
        An Iterator that iterates though the files and directories.

    """

    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    if None is path_or_prefix:
        path_or_prefix = ""

    (handler, actual_path) = default_context.get_handler(path_or_prefix)
    return handler.list(actual_path, recursive)


def info() -> str:
    """Shows the detail of the current default handler

    Please refer to the :func:`set_root` for details about the default handler.

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

    If an Unix path is given, the method use the default handler,
    and identifies the file from root path.
    See :func:`set_root` for details about root path and default handler.
    If an URI is given, the filesystem is chosen according to the given scheme.

    The function returns a file object, and the type depends on
    the filesystem of the file and ``mode``.
    For HDFS, the return type is the same as the POSIX/built-in case.

    Args:
        file_path (str): the target file path, can be an Unix path,
            or an URI.

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
        A file object according to the filesystem and ``mode``.

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
    ChainerIO uses when called without a full URI. Overrides by the full URI.
    The default handler can be created by :func:`create_handler` with
    the name of the handler.

    Example::

        # Case 1
        # set_root by the name of uri 
        chainerio.set_root("posix")
        # open a file on posix filesystem with path "some/file"
        chainerio.open("some/file")
        # override with a full uri
        chainerio.open("hdfs:///some/file/on/hdfs")

        # Case 2
        # set_root by uri
        chainerio.set_root("hdfs:///some/directory")
        # open a file "on/hdfs" on hdfs under the " some/directory"
        chainerio.open("on/hdfs")

        # Case 3
        handler = chainerio.create_handler('hdfs')
        # set_root by handler
        chainerio.set_root(handler)
        # open a file "/some/file/on/hdfs" on hdfs
        chainerio.open("/some/file/on/hdfs")

        # Case 4
        handler = chainerio.open_as_container('some.zip')
        # set_root by handler
        chainerio.set_root(handler)
        # open a file "img.jpg" in "some.zip"
        chainerio.open("img.jpg")

    The root path refers to a directory that ChainerIO works on.
    It is similar to current working directory, ``CWD`` in terms of the shell
    environment.
    The root path will only be set
    when the ``uri_or_handler`` points to a directory.
    Otherwise, it will be set to default,
    which represents the default working directory as follow:

    +---------+---------------------------+
    |         |     Default Root Path     |
    +=========+===========================+
    | POSIX   | current working directory |
    +---------+---------------------------+
    | HDFS    |     /user/USERNAME        |
    +---------+---------------------------+
    | zip     |      top directory        |
    +---------+---------------------------+

    Args:
        uri_or_handler (str or :class:~`chainerio.IO`): The ``uri_or_handler``
            can accept the following three kinds of values:

            1. the name of a handler (string): set the default handler
                to the corresponding handler, and root path.
                See :func:`create_handler` for supported name of handlers.

            2. an uri of directory (string): set the context
                to use the corresponding handler and set
                the root path to the given directory.

            3. a handler, which is an instance of
                :class:`chainerio.IO`. Set the default handler
                to the given handler, and root path to default.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    default_context.set_root(uri_or_handler)


def create_handler(handler_name: str) -> IO:
    """Returns a handler according to the given ``handler_name``

    The current supported handlers are:

    1. posix
    2. hdfs

    Args:
        handler_name (str): the name of handler to create

    Returns:
        An object of :class:`chainerio.IO`
        according to the ``handler_name``

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(handler_name)
    return handler


def isdir(path: str) -> bool:
    """Returns ``Ture`` if the path is an existing directory

    Args:
        path (str): the path to the target directory
            The ``path`` can be an Unix path or an URI.

    Returns:
        ``True`` when the path points to a directory, ``False`` when it is not

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.isdir(actual_path)


def mkdir(path: str, mode: int = 0o777, *, dir_fd: Optional[int] = None) -> None:
    """Makes a directory with mode

    Args:
        path (str): the path to the directory to make
            The ``path`` can be an Unix path or an URI.

        mode (int): the mode of the new directory

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.mkdir(actual_path, mode, dir_fd=dir_fd)


def makedirs(path: str, mode: int = 0o777,
             exist_ok: bool = False) -> None:
    """Makes directories recursively with mode

    Also creates all the missing parents of the given path.

    Args:
        path (str): the path to the directory to make.
            The ``path`` can be  an Unix path or an URI.

        mode (int): the mode of the directory

        exist_ok (bool): In default case, a `FileExitsError` will be raised
            when the target directory exists. Set the ``exist_ok`` to surpass

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.makedirs(actual_path, mode, exist_ok)


def exists(path: str) -> bool:
    """Returns ``True`` when the given ``path`` exists

    Args:
        path (str): the ``path`` to the target file. The ``path`` can be an
        Unix path or an URI.

    Returns:
        ``True`` when the file or directory exists, ``False`` when it is not.

    """
    global _DEFAULT_CONTEXT
    default_context = _DEFAULT_CONTEXT

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.exists(actual_path)


def rename(src: str, dst: str) -> None:
    """Renames the file from ``src`` to ``dst``

    Note the ``src`` and ``dst`` SHOULD be in the same filesystem.
    The ``src`` and ``dst`` can be either Unix paths or URIs.

    Args:
        src (str): the current name of the target file or directory.

        dst (str): the name to rename to.

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

    A combination of :func:`os.remove` and :func:`os.rmtree`.

    Args:
        path (str): the target path to remove. The ``path`` can be a
        regular file or a directory.
        The ``path`` can be an Unix file path or an URI.

        recursive (bool): When the given path is a directory, all the files
            and directories under it will be removed.
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
