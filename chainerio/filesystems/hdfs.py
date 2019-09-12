from chainerio.filesystem import FileSystem
from chainerio.io import open_wrapper
from krbticket import KrbTicket

import getpass
import io
import logging
import os
import pyarrow
from pyarrow import hdfs

from typing import Type, Optional, Callable, Any

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class HdfsFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root="", keytab_path=None):
        FileSystem.__init__(self, io_profiler, root)
        self.connection = None
        self.type = 'hdfs'
        self.root = root
        self.username = getpass.getuser()
        self.userid = os.getuid()
        self.keytab_path = keytab_path

    def _create_connection(self):
        if None is self.connection:
            logger.debug('creating connection')

            if None is not self.keytab_path:
                self.ticket = KrbTicket.init(
                    self.username,
                    self.keytab_path)
                self.ticket.updater_start()

            connection = hdfs.connect()
            assert connection is not None
            self.connection = connection

    def _dump_read_file(self, filepath, content):
        abs_path = os.path.join(self.local_dump_dir, filepath)
        directory, basename = os.path.split(abs_path)

        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(abs_path, 'wb') as dump_file:
            dump_file.write(content)
        return

    def _wrap_fileobject(self, file_obj: Type['IOBase'],
                         file_path: str, mode: str = 'rb',
                         buffering: int = -1,
                         encoding: Optional[str] = None,
                         errors: Optional[str] = None,
                         newline: Optional[str] = None,
                         closefd: bool = True,
                         opener: Optional[Callable[
                             [str, int], Any]] = None) -> Type['IOBase']:

        if 'b' not in mode:
            file_obj = io.TextIOWrapper(file_obj, encoding, errors, newline)
        elif 'r' in mode:
            # Wrapping file_obj with io.BufferedReader to add `peek` support,
            # which signiciantly improves unpickle performance.
            file_obj = io.BufferedReader(file_obj)
        else:
            file_obj = io.BufferedWriter(file_obj)

        return file_obj

    @open_wrapper
    def open(self, file_path, mode='rb',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        self._create_connection()

        # hdfs only support open in 'b'
        if 'b' not in mode:
            mode += 'b'
        try:
            hdfs_file = self.connection.open(file_path, mode)
            return hdfs_file
        except pyarrow.lib.ArrowIOError as e:
            raise IOError("open file error :{}".format(str(e)))

    def close(self):
        self._close_connection()

    def info(self):
        # a placeholder
        info_str = "using hdfs"
        return info_str

    def list(self, path_or_prefix: str = None, recursive=False):
        if path_or_prefix is None:
            path_or_prefix = "/user/{}".format(self.username)

        self._create_connection()

        if recursive:
            yield from self._recursive_list(path_or_prefix, path_or_prefix)
        else:
            dir_list = self.connection.ls(path_or_prefix)
            for _dir in dir_list:
                yield os.path.basename(_dir)

    def _recursive_list(self, path_or_prefix, path):
        for _file in self.connection.ls(path, detail=True):
            file_name = _file['name']
            # convert the full URI to relative path from path_or_prefix
            # to align with posix
            # e.g. "hdfs://nameservice/prefix_dir/testfile"
            # => "prefix_dir/testfile"
            yield file_name[file_name.find(path_or_prefix):]

            if 'directory' == _file['kind']:
                yield from self._recursive_list(path_or_prefix, file_name)

    def stat(self, path):
        self._create_connection()
        return self.connection.stat(path)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close_connection()

    def _close_connection(self):
        if None is not self.connection:
            self.connection.close()
            self.connection = None

    def isdir(self, file_path: str):
        stat = self.stat(file_path)
        return "directory" == stat["kind"]

    def mkdir(self, file_path: str, *args, dir_fd=None):
        self._create_connection()
        return self.connection.mkdir(file_path)

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        return self.mkdir(file_path, mode, exist_ok)

    def exists(self, file_path: str):
        self._create_connection()
        return self.connection.exists(file_path)

    # not used for now, save for the future use
    def read(self, file_path, mode='rb'):
        # support rb open only

        with self.open(file_path, mode) as file_obj:
            return file_obj.read()

    def write(self, file_path, content, mode='wb'):
        # support wb open only

        content = self._convert_to_bytes(content)
        with self.open(file_path, "wb") as file_obj:
            return file_obj.write(content)

    def rename(self, src, dst):
        self._create_connection()
        return self.connection.rename(src, dst)

    def remove(self, path, recursive=False):
        self._create_connection()
        return self.connection.delete(path, recursive)
