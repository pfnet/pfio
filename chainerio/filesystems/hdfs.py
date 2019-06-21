from chainerio.fileobject import FileObject
from chainerio.filesystem import FileSystem
from chainerio.io import open_wrapper
from chainerio.profiler import profiling_decorator

from krbticket import KrbTicket

import getpass
import io
import logging
import os
import pyarrow
from pyarrow import hdfs


class HdfsFileObject(FileObject):
    def __init__(self, base_file_object, base_filesystem_handler,
                 io_profiler, file_path: str, mode: str = "r", buffering=-1,
                 encoding=None, errors=None, newline=None,
                 closefd=True, opener=None):
        if 'b' not in mode:
            base_file_object = io.TextIOWrapper(base_file_object,
                                                encoding, errors, newline)
        FileObject.__init__(self, base_file_object, base_filesystem_handler,
                            io_profiler=io_profiler, file_path=file_path,
                            mode=mode, buffering=buffering, encoding=encoding,
                            errors=errors, newline=newline, closefd=closefd,
                            opener=opener)

    @profiling_decorator
    def readline(self):
        if (not isinstance(self.base_file_object, io.BytesIO) and
                'b' in self.mode):
            # supporting the readline in bytes mode
            self.base_file_object = io.BytesIO(
                self.base_file_object.read())
        return self.base_file_object.readline()


class HdfsFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root="", keytab_path=None):
        FileSystem.__init__(self, io_profiler, root)
        self.connection = None
        self.type = 'hdfs'
        self.root = root
        self.username = getpass.getuser()
        self.userid = os.getuid()
        self.keytab_path = keytab_path
        self.fileobj_class = HdfsFileObject

    def _create_connection(self):
        if None is self.connection:
            logging.debug('creating connection')

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

    @open_wrapper
    @profiling_decorator
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

    @profiling_decorator
    def close(self):
        self._close_connection()

    @profiling_decorator
    def info(self):
        # a placeholder
        info_str = "using hdfs"
        return info_str

    @profiling_decorator
    def list(self, path_or_prefix: str = None):
        if not path_or_prefix or None is path_or_prefix:
            path_or_prefix = "/user/{}".format(self.username)

        self._create_connection()
        dir_list = self.connection.ls(path_or_prefix)
        for _dir in dir_list:
            yield os.path.basename(_dir)

    @profiling_decorator
    def stat(self, path):
        self._create_connection()
        return self.connection.stat(path)

    @profiling_decorator
    def __enter__(self):
        return self

    @profiling_decorator
    def __exit__(self, type, value, traceback):
        self._close_connection()

    @profiling_decorator
    def _close_connection(self):
        if None is not self.connection:
            self.connection.close()
            self.connection = None

    @profiling_decorator
    def isdir(self, file_path: str):
        stat = self.stat(file_path)
        return "directory" == stat["kind"]

    @profiling_decorator
    def mkdir(self, file_path: str, *args, dir_fd=None):
        self._create_connection()
        return self.connection.mkdir(file_path)

    @profiling_decorator
    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        return self.mkdir(file_path, mode, exist_ok)

    @profiling_decorator
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

    @profiling_decorator
    def rename(self, src, dst):
        self._create_connection()
        return self.connection.rename(src, dst)

    @profiling_decorator
    def remove(self, path, recursive=False):
        self._create_connection()
        return self.connection.delete(path, recursive)
