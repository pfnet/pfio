import os
import re
from typing import Tuple

from pfio._typing import Union
from pfio.container import Container
from pfio.io import IO, create_fs_handler


class FileSystemDriverList(object):
    def __init__(self):
        # TODO(tianqi): dynamically create this list
        # as well as the patterns upon loading the pfio module.
        self.scheme_list = ["hdfs", "posix"]
        self.posix_pattern = re.compile(r"file:\/\/(?P<path>.+)")
        self.hdfs_pattern = re.compile(r"(?P<path>hdfs:\/\/.+)")
        self.pattern_list = {"hdfs": self.hdfs_pattern,
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

    def get_handler_from_path(self, path: str) -> Tuple[IO, str, bool]:
        (fs_type, actual_path, is_URI) = self._determine_fs_type(path)

        handler = create_fs_handler(fs_type)
        return (handler, actual_path, is_URI)

    def get_handler_for_root(self,
                             uri_or_handler_name: str) -> Tuple[IO, str, bool]:
        if uri_or_handler_name in self.pattern_list.keys():
            return (create_fs_handler(uri_or_handler_name), "", False)
        else:
            (new_handler, actual_path, is_URI) = self.get_handler_from_path(
                uri_or_handler_name)
            new_handler.root = actual_path
            return (new_handler, actual_path, is_URI)

    def is_supported_scheme(self, scheme: str) -> bool:
        return scheme in self.scheme_list


class DefaultContext(object):
    def __init__(self):
        self._fs_handler_list = FileSystemDriverList()
        self._root = ""

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

    def is_supported_scheme(self, scheme: str) -> bool:
        return self._fs_handler_list.is_supported_scheme(scheme)
