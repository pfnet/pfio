from chainerio._context import context
from chainerio._context import using_config  # NOQA
from chainerio.version import __version__  # NOQA
from chainerio.profiler import profiling  # NOQA

from chainerio.io import IO
from typing import Optional, Iterator, Any, Callable

from chainerio.fileobject import FileObject


def open_as_container(path: str) -> IO:
    default_context = context

    return default_context.open_as_container(path)


def list(path_or_prefix: Optional[str] = None) -> Iterator:
    """list all the files under the given path_or_prefix

    The default value for parameter of list is None
    When we get the default value None, list behave similar to
    the default os.listdir: it shows the content under the root directory,
    which is set to local $HOME, as the default value.
    Please note that the meaning of $HOME depends on each filesystem.
    For example in case of any containers, it behaves like the namelist().
    However, if a `path_or_prefix` is given,
    then it shows all the files that start with `path_or_prefix`
    """

    default_context = context

    if None is path_or_prefix:
        path_or_prefix = ""

    (handler, actual_path) = default_context.get_handler(path_or_prefix)
    return handler.list(actual_path)


def info() -> str:
    global context
    default_context = context

    (handler, dummy_path) = default_context.get_handler()
    return handler.info()


def open(file_path: str, mode: str = 'rb',
         buffering: int = -1, encoding: Optional[str] = None,
         errors: Optional[str] = None, newline: Optional[str] = None,
         closefd: bool = True,
         opener: Optional[Callable[
             [str, int], Any]] = None) -> FileObject:
    default_context = context

    (handler, actual_path) = default_context.get_handler(file_path)
    return handler.open(
        actual_path, mode, buffering, encoding,
        errors, newline, closefd, opener)


def set_root(uri_or_handler: str) -> None:
    default_context = context

    default_context.set_root(uri_or_handler)


def create_handler(uri: str) -> IO:
    default_context = context

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(uri)
    return handler


def isdir(file_path: str) -> bool:
    default_context = context

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(file_path)
    return handler.isdir(actual_path)


def mkdir(file_path: str, mode: int = 0o777, **kwargs) -> None:
    default_context = context

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(file_path)
    return handler.mkdir(actual_path, mode, *kwargs)


def makedirs(file_path: str, mode: int = 0o777,
             exist_ok: bool = False) -> None:
    default_context = context

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(file_path)
    return handler.makedirs(actual_path, mode, exist_ok)


def exists(file_path: str) -> bool:
    default_context = context

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(file_path)
    return handler.exists(actual_path)


def rename(src: str, dst: str) -> None:
    default_context = context

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
    default_context = context

    (handler, actual_path, is_URI) = \
        default_context.get_handler_by_name(path)
    return handler.remove(actual_path, recursive)


def get_root_dir() -> str:
    default_context = context

    return default_context.get_root_dir()
