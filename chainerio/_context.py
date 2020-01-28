from chainerio.container import Container
from chainerio.io import create_fs_handler
from chainerio.io import IO
from threading import Lock
import re

from chainerio._typing import Union
from typing import Tuple


class FileSystemDriverList(object):
    def __init__(self):
        # TODO(tianqi): dynamically create this list
        # as well as the patterns upon loading the chainerio module.
        self.scheme_list = ["hdfs", "posix"]
        self.posix_pattern = re.compile(r"file:\/\/(?P<path>.+)")
        self.hdfs_pattern = re.compile(r"hdfs:\/\/(?P<path>.+)")
        self.pattern_list = {"hdfs": self.hdfs_pattern,
                             "posix": self.posix_pattern, }
        # this is a cache to store the handler for global context
        self._handler_cache = {}
        self._handler_mt_lock = Lock()

    def format_path(self, path: str) -> Tuple[str, str, bool]:
        if path in self.scheme_list:
            # when the path is just a scheme
            return (path, "", True)

        for fs_type, pattern in self.pattern_list.items():
            ret = pattern.match(path)
            if ret:
                return (fs_type, ret.groupdict()["path"], True)

        return ("posix", path, False)

    def get_handler(self, fs_type: str) -> Tuple[IO]:

        self._handler_mt_lock.acquire()

        if fs_type in self._handler_cache:
            # get handler from cache
            handler = self._handler_cache[fs_type]
        else:
            # create a new handler
            handler = create_fs_handler(fs_type)
            self._handler_cache[fs_type] = handler

        self._handler_mt_lock.release()

        return handler

    def is_supported_scheme(self, scheme: str) -> bool:
        return scheme in self.scheme_list


class DefaultContext(object):
    def __init__(self):
        self._fs_handler_list = FileSystemDriverList()

        self._default_context = \
            self._fs_handler_list.get_handler("posix")

    def set_root(self, uri_or_handler: Union[str, IO]) -> None:
        if isinstance(uri_or_handler, IO):
            handler = uri_or_handler
        else:
            handler, path = self.get_handler(uri_or_handler)
            handler.root = path

            if handler.root:
                if not handler.isdir(handler.root):
                    raise RuntimeError("""the URI '{}' does not
                        point to a directory""".format(handler.root))

        self._default_context = handler

    def get_handler(self, path: str = "") -> Tuple[IO, str]:
        (fs_type, actual_path, is_URI) = \
            self._fs_handler_list.format_path(path)

        if not is_URI:
            handler = self._default_context
        else:
            handler = self._fs_handler_list.get_handler(fs_type)
        return (handler, actual_path)

    def open_as_container(self, path: str) -> Container:
        (handler, actual_path) = self.get_handler(path)
        return handler.open_as_container(actual_path)

    def get_root_dir(self) -> str:
        return self._default_context.root

    def is_supported_scheme(self, scheme: str) -> bool:
        return self._fs_handler_list.is_supported_scheme(scheme)
