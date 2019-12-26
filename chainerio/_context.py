from chainerio.container import Container
from chainerio.io import create_fs_handler
from chainerio.io import IO
import os
import re

from chainerio._typing import Union
from typing import Tuple


class FileSystemDriverList(object):
    def __init__(self):
        # TODO(tianqi): dynamically create this list
        # as well as the patterns upon loading the chainerio module.
        self.scheme_list = ["hdfs", "posix"]
        self.posix_pattern = re.compile(r"file:\/\/(?P<path>.+)")
        self.hdfs_pattern = re.compile(r"(?P<path>hdfs:\/\/.+)")
        self.pattern_list = {"hdfs": self.hdfs_pattern,
                             "posix": self.posix_pattern, }

    def format_path(self, path: str) -> Tuple[str, str, bool]:
        if path in self.scheme_list:
            # when the path is just a scheme
            return (path, "", True)

        for fs_type, pattern in self.pattern_list.items():
            ret = pattern.match(path)
            if ret:
                return (fs_type, ret.groupdict()["path"], True)

        return ("posix", path, False)

    def _create_handler_from_path(self, path: str,
                                  fs_type: str = None) -> Tuple[IO, str, bool]:
        if fs_type is None:
            (fs_type, actual_path, is_URI) = self.format_path(path)
        else:
            actual_path = path
            is_URI = False

        handler = create_fs_handler(fs_type)
        return (handler, actual_path, is_URI)

    def get_handler(self, uri_or_handler_name: str) -> Tuple[IO, str, bool]:
        if uri_or_handler_name in self.pattern_list.keys():
            return (create_fs_handler(uri_or_handler_name), "", False)
        else:
            (new_handler, actual_path, is_URI) = \
                self._create_handler_from_path(uri_or_handler_name)
            new_handler.root = actual_path
            return (new_handler, actual_path, is_URI)

    def is_supported_scheme(self, scheme: str) -> bool:
        return scheme in self.scheme_list


class DefaultContext(object):
    def __init__(self):
        self._fs_handler_list = FileSystemDriverList()

        self._default_context = \
            self._fs_handler_list.get_handler("posix")[0]

        self._global_handler_cache = {}

    def set_root(self, uri_or_handler: Union[str, IO]) -> None:
        if isinstance(uri_or_handler, IO):
            handler = uri_or_handler
        else:
            handler, path = self._get_handler_no_root(uri_or_handler)
            handler.root = path

            if handler.root:
                if not handler.isdir(handler.root):
                    raise RuntimeError("""the URI '{}' does not
                        point to a directory""".format(handler.root))

        self._default_context = handler

    def _get_handler_no_root(self, path: str = "") -> Tuple[IO, str]:
        (fs_type, path, is_URI) = self._fs_handler_list.format_path(path)

        if not is_URI:
            handler = self._default_context
        else:
            if fs_type in self._global_handler_cache:
                # get handler from cache
                handler = self._global_handler_cache[fs_type]
            else:
                # create a new handler
                handler = create_fs_handler(fs_type)
                self._global_handler_cache[fs_type] = handler
        return (handler, path)

    def get_handler(self, path: str = "") -> Tuple[IO, str]:
        handler, path = self._get_handler_no_root(path)
        actual_path = os.path.join(handler.root, path)
        return (handler, actual_path)

    def open_as_container(self, path: str) -> Container:
        (handler, actual_path) = self.get_handler(path)
        return handler.open_as_container(actual_path)

    def get_root_dir(self) -> str:
        return self._default_context.root

    def is_supported_scheme(self, scheme: str) -> bool:
        return self._fs_handler_list.is_supported_scheme(scheme)
