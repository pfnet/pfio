from chainerio.container import Container
from chainerio.io import create_fs_handler
from chainerio.io import IO
import os
import re

from typing import Tuple, Union, Any


class FileSystemDriverList(object):
    def __init__(self):
        self._handler_dict = {}

        self.posix_pattern = re.compile(r"file:\/\/(?P<path>.+)")
        self.hdfs_pattern = re.compile(r"(?P<path>hdfs:\/\/.+)")
        self.http_pattern = re.compile(r"(?P<path>(http:\/\/|https:\/\/).+)")
        self.pattern_list = {"hdfs": self.hdfs_pattern,
                             "http": self.http_pattern,
                             "posix": self.posix_pattern, }

    def _determine_fs_type(self, path: str) -> Tuple[str, str, bool]:
        if None is not path:
            for fs_type, pattern in self.pattern_list.items():
                ret = pattern.match(path)
                if ret:
                    return (fs_type, ret.groupdict()["path"], True)

        return ("posix", path, False)

    def format_path(self, fs: IO, path: str) -> Tuple[str, bool]:
        fs_type = fs.type
        if fs_type in self.pattern_list.keys():
            pattern = self.pattern_list[fs_type]
            ret = pattern.match(path)
            if ret:
                return (ret.groupdict()["path"], True)
            else:
                return (path, False)
        else:
            return (path, False)

    def _get_handler(self, fs_type: str) -> IO:
        if fs_type in self._handler_dict.keys():
            return self._handler_dict[fs_type]
        else:
            new_handler = create_fs_handler(fs_type=fs_type)
            self._handler_dict[fs_type] = new_handler
            return new_handler

    def get_handler_from_path(self, path: str) -> Tuple[IO, str, bool]:
        (fs_type, actual_path, is_URI) = self._determine_fs_type(path)

        handler = self._get_handler(fs_type)
        return (handler, actual_path, is_URI)

    def get_handler_for_root(self,
                             uri_or_handler_name: str) -> Tuple[IO, str, bool]:
        if uri_or_handler_name in self.pattern_list.keys():
            return (self._get_handler(uri_or_handler_name), "", False)
        else:
            (new_handler, actual_path, is_URI) = self.get_handler_from_path(
                uri_or_handler_name)
            new_handler.root = actual_path
            return (new_handler, actual_path, is_URI)


class DefaultContext(object):
    def __init__(self):
        self._fs_handler_list = FileSystemDriverList()
        self._root = ""
        self._profiling = False

        self._default_context = \
            self._fs_handler_list.get_handler_for_root("posix")[0]

    def set_root(self, uri_or_handler: Union[str, IO]) -> None:
        # TODO(check) if root is directory
        if isinstance(uri_or_handler, IO):
            handler = uri_or_handler
            self._root = ""
        else:
            (handler, self._root, is_URI) = \
                self.get_handler_by_name(uri_or_handler)
        assert handler is not None

        if self._root:
            if not handler.isdir(self._root):
                raise RuntimeError("the URI does not point to a directory")

        self._default_context = handler

    def get_handler(self, path: str = "") -> Tuple[IO, str]:
        (handler, formatted_path,
         is_URI) = self._fs_handler_list.get_handler_from_path(path)

        if not is_URI:
            actual_path = os.path.join(self._root, formatted_path)
            return (self._default_context, actual_path)
        else:
            return (handler, formatted_path)

    def open_as_container(self, path: str) -> Container:
        (handler, formatted_path,
         is_URI) = self._fs_handler_list.get_handler_from_path(path)

        if not is_URI:
            actual_path = os.path.join(self._root, formatted_path)
            handler = self._default_context
        else:
            actual_path = formatted_path

        self._root = ""
        return handler.open_as_container(actual_path)

    def get_handler_by_name(self, path: str) -> Tuple[IO, str, bool]:
        return self._fs_handler_list.get_handler_for_root(path)

    def get_root_dir(self) -> str:
        return self._root


context = DefaultContext()


class _ConfigContext(object):

    old_value = None

    def __init__(self, config, name, value):
        self.config = config
        self.name = name
        self.value = value

    def __enter__(self):
        name = self.name
        value = self.value
        config = self.config

        setattr(config, name, value)

    def __exit__(self, typ, value, traceback):
        setattr(self.config, self.name, self.old_value)


def using_config(name: str, value: Any,
                 config=context) -> '_ConfigContext':
    """using_config(name, value, config=chainer.config)

    Context manager to temporarily change the thread-local configuration.

    Args:
        name (str): Name of the configuration to change.
        value: Temporary value of the configuration entry.
        config (~chainer.configuration.LocalConfig): Configuration object.
            Chainer's thread-local configuration is used by default.

    .. seealso::
        :ref:`configuration`

    """
    return _ConfigContext(config, name, value)
