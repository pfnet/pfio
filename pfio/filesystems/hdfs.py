import getpass
import io
import logging
import os
import re
import subprocess
from io import IOBase
from typing import Any, Callable, Type

import pyarrow
from pyarrow import hdfs

from pfio._typing import Optional
from pfio.filesystem import FileSystem
from pfio.io import FileStat, open_wrapper

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


def _parse_principal_name_from_klist(output):
    output_array = output.split('\n')
    if len(output_array) < 2:
        return None

    principle_str = output_array[1]
    klist_principal_pattern = re.compile(
        r'Default principal: (?P<username>.+)@(?P<service>.+)')
    ret = klist_principal_pattern.match(principle_str)
    if ret:
        pattern_dict = ret.groupdict()
        return pattern_dict['username']
    else:
        return None


def _parse_principal_name_from_keytab(output):
    output_array = output.split('\n')
    if len(output_array) < 4:
        return None

    principle_str = output_array[3]
    keytab_principle_pattern = re.compile(
        r'\s+\d+ (?P<username>.+)@(?P<service>.+)')
    ret = keytab_principle_pattern.match(principle_str)
    if ret:
        pattern_dict = ret.groupdict()
        return pattern_dict['username']
    else:
        return None


def _get_principal_name_from_keytab():
    output = _run_klist(use_keytab=True)
    if output is None:
        return None

    return _parse_principal_name_from_keytab(output.decode('utf-8'))


def _get_principal_name_from_klist():
    output = _run_klist()
    if output is None:
        return None
    return _parse_principal_name_from_klist(output.decode('utf-8'))


def _run_klist(use_keytab=False):
    try:
        command = ['klist']
        if use_keytab:
            command += ['-k']
        pipe = subprocess.Popen(command, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = pipe.communicate()
        if out == b'' and err != b'':
            return None
        else:
            return out
    except OSError:
        # klist is not found
        return None


class HdfsFileStat(FileStat):
    """Detailed information of a file in HDFS

    Attributes:
        filename (str): Derived from `~FileStat`.
        last_modifled (float): Derived from `~FileStat`.
            No sub-second precision.
        last_accessed (float): UNIX timestamp of last access time.
            No sub-second precision.
        mode (int): Derived from `~FileStat`.
        size (int): Derived from `~FileStat`.
        owner (str): Owner of the file. Unlike `~PosixFileStat.owner`,
            this is a user name string instead of an integer.
        group (str): Group of the file in string.
        replication (int): Number of replications of the file in HDFS.
        block_size (int): Block size in bytes.
        kind (str): Group of the file in string.
    """

    def __init__(self, info):
        mode = info['permissions']
        if info['kind'] == 'file':
            mode |= 0o100000
        elif info['kind'] == 'directory':
            mode |= 0o40000

        self.filename = info['path']
        self.mode = mode
        self.last_modified = float(info['last_modified'])
        self.last_accessed = float(info['last_accessed'])
        for k in ('size', 'owner', 'group', 'replication',
                  'block_size', 'kind'):
            setattr(self, k, info[k])


class HdfsFileSystem(FileSystem):
    def __init__(self, io_profiler=None, root=""):
        FileSystem.__init__(self, io_profiler, root)
        self.connection = None
        self.type = 'hdfs'
        self.root = root
        self.username = self._get_principal_name()

        self.nameservice = None

    def _get_principal_name(self):
        # get the default principal name from `klist` cache
        principal_name = _get_principal_name_from_klist()

        if principal_name is not None:
            return principal_name

        # try getting principal name from keytab
        principal_name = _get_principal_name_from_keytab()
        if principal_name is not None:
            return principal_name

        # in case every thing, use the login username instead
        return self._get_login_username()

    def _get_login_username(self):
        return getpass.getuser()

    def _create_connection(self):
        if None is self.connection:
            logger.debug('creating connection')

            connection = hdfs.connect()
            assert connection is not None
            self.connection = connection

            # set nameservice
            _file_in_root = self.connection.ls("/")[0]
            self.nameservice = _file_in_root[:_file_in_root.rfind("/")]

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
        target_dir = self.connection.info(path_or_prefix)
        if target_dir['kind'] != "directory":
            raise NotADirectoryError(path_or_prefix)

        target_dir_path = target_dir['path']
        # +1 to include the "/"
        full_uri_prefix_offset = len(target_dir_path) + 1

        if recursive:
            yield from self._recursive_list(full_uri_prefix_offset,
                                            path_or_prefix)
        else:
            dir_list = self.connection.ls(path_or_prefix)
            for _dir in dir_list:
                yield os.path.basename(_dir)

    def _recursive_list(self, full_uri_prefix_offset, path):
        for _file in self.connection.ls(path, detail=True):
            file_name = _file['name']
            # convert the full URI to relative path from path_or_prefix
            # to align with posix
            # e.g. "hdfs://nameservice/prefix_dir/testfile"
            # => "testfile"
            yield file_name[full_uri_prefix_offset:]

            if 'directory' == _file['kind']:
                yield from self._recursive_list(full_uri_prefix_offset,
                                                file_name)

    def stat(self, path):
        self._create_connection()
        return HdfsFileStat(self.connection.info(path))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._close_connection()

    def _close_connection(self):
        if None is not self.connection:
            self.connection.close()
            self.connection = None

    def isdir(self, file_path: str):
        return self.stat(file_path).isdir()

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
