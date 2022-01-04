import getpass
import io
import logging
import os
import re
import subprocess
from xml.etree import ElementTree

import pyarrow
from pyarrow.fs import FileSelector, FileType, HadoopFileSystem

from .fs import FS, FileStat

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
    """

    def __init__(self, info):
        self._info = info

        mode = 0
        if info.type == FileType.File:
            mode |= 0o100000
        elif info.type == FileType.Directory:
            mode |= 0o40000

        self.filename = info.base_name
        self.mode = mode
        self.last_modified = info.mtime.timestamp()
        # no atime supported by PyArrow new API
        self.last_accessed = info.mtime.timestamp()
        self.size = info.size


def _ensure_arrow_envs(hadoop_home):
    if os.getenv("ARROW_LIBHDFS_DIR") is None:
        arrow_dir = os.path.join(hadoop_home, "lib")
        libhdfs = os.path.join(arrow_dir, "libhdfs.so")

        if os.path.exists(libhdfs):
            os.environ["ARROW_LIBHDFS_DIR"] = arrow_dir
        else:
            arrow_dir = os.path.join(hadoop_home, "lib/native")
            libhdfs = os.path.join(arrow_dir, "libhdfs.so")
            if os.path.exists(libhdfs):
                os.environ["ARROW_LIBHDFS_DIR"] = arrow_dir
            else:
                msg = "No libhdfs.so found from $HADOOP_HOME: {}".format(
                    hadoop_home)
                raise RuntimeError(msg)


def _create_fs():
    confdir = os.getenv('HADOOP_CONF_DIR', '/etc/hadoop/conf')
    conffile = os.path.join(confdir, 'hdfs-site.xml')
    root = ElementTree.parse(conffile)

    # Valid envs required. Typically, /opt/cloudera/parcels/CDH/lib
    if os.getenv("HADOOP_HOME"):
        hadoop_home = os.getenv("HADOOP_HOME")
        _ensure_arrow_envs(hadoop_home)

    if os.getenv("CLASSPATH") is None:
        # TODO(kuenishi): CLASSPATH=`$HADOOP_HOME/bin/hdfs classpath --glob`
        cmd = ["hdfs", "classpath", "--glob"]
        cp = subprocess.run(cmd, stdout=subprocess.PIPE)
        cp.check_returncode()
        os.environ["CLASSPATH"] = cp.stdout.decode()

    assert os.getenv("ARROW_LIBHDFS_DIR"), "LIBHDFS not found"
    assert os.getenv("CLASSPATH"), "CLASSPATH not defined"

    configs = {}
    for e in root.findall('./property'):
        name = None
        for c in e:
            if c.tag == 'name':
                name = c.text
            elif c.tag == 'value':
                value = c.text
        if name:
            configs[name] = value

    for nameservice in configs.get('dfs.nameservices', '').split(','):

        # TODO(kuenishi): We don't have such use case where we switch
        # amont multiple name services from single HADOOP_CONF_DIR
        # conf. Thus we ignore fs.defaultFS and just take the very
        # first name service that appeared in hdfs-site.xml.
        return HadoopFileSystem(nameservice, 0)

    else:
        RuntimeError("No nameservice found.")


class Hdfs(FS):
    '''Hadoop FileSystem wrapper

    To use HDFS, PFIO requires ``$HADOOP_HOME`` predefined before
    initialization. If it is not defined, ``ARROW_LIBHDFS_DIR`` must
    be defined instead. ``$CLASSPATH`` will be needed in case ``hdfs``
    command is not available from ``$PATH``.

    .. note:: With environment variable
          ``KRB5_KTNAME=path/to/your.keytab`` set, ``hdfs``
          handler automatically starts automatic and periodical
          updating Kerberos ticket using `krbticket
          <https://pypi.org/project/krbticket/>`_ . The update
          frequency is every 10 minutes by default.
    .. note::
          Only the username in the first entry in The
          keytab will be used to update the Kerberos ticket.
    '''

    def __init__(self, cwd=None, create=False, **_):
        super().__init__()
        self._fs = _create_fs()
        assert self._fs is not None
        self.username = self._get_principal_name()

        self.cwd = os.path.join('/user', self.username)

        if cwd is not None:
            if cwd.startswith('/'):
                self.cwd = cwd
            else:
                self.cwd = os.path.join(self.cwd, cwd)

        if not self.isdir(''):
            if create:
                self.makedirs('')
            else:
                raise ValueError('{} must be a directory'.format(self.cwd))

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

    def open(self, file_path, mode='rb',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):
        self._checkfork()
        path = os.path.join(self.cwd, file_path)

        try:
            if 'r' in mode:
                file_obj = self._fs.open_input_file(path)
            else:
                file_obj = self._fs.open_output_stream(path)
        except pyarrow.lib.ArrowIOError as e:
            raise IOError("open file error :{}".format(str(e)))

        return self._wrap_file_obj(file_obj, mode, encoding, errors, newline)

    def _wrap_file_obj(self, file_obj, mode, encoding, errors, newline):
        if 'b' not in mode:
            return io.TextIOWrapper(file_obj, encoding, errors, newline)
        elif 'r' in mode:
            # Wrap file_obj with io.BufferedReader for ``peek()``, to
            # significiantly improve unpickle performance.
            return io.BufferedReader(file_obj)
        elif 'w' in mode:
            return io.BufferedWriter(file_obj)
        else:
            raise ValueError("invalid option")

    def subfs(self, rel_path):
        return Hdfs(os.path.join(self.cwd, rel_path))

    def close(self):
        pass

    def list(self, path: str = "", recursive=False):
        self._checkfork()

        if not self.isdir(path):
            raise NotADirectoryError(path)

        path = os.path.join(self.cwd, path)
        norm_path = self._fs.normalize_path(path).rstrip('/')

        infos = self._fs.get_file_info(FileSelector(path, recursive=recursive))
        for file_info in infos:
            yield file_info.path[len(norm_path)+1:]

    def stat(self, path):
        self._checkfork()
        path = os.path.join(self.cwd, path)
        info = self._fs.get_file_info(path)
        if info.type == FileType.NotFound:
            raise FileNotFoundError()
        else:
            return HdfsFileStat(info)

    def isdir(self, path: str):
        self._checkfork()
        path = os.path.join(self.cwd, path)
        info = self._fs.get_file_info(path)
        return info.type == FileType.Directory

    def mkdir(self, path: str, *args, dir_fd=None):
        self._checkfork()
        path = os.path.join(self.cwd, path)
        return self._fs.create_dir(path)

    def makedirs(self, path: str, mode=0o777, exist_ok=False):
        self._checkfork()
        if self.exists(path) and self.isdir(path) and not exist_ok:
            raise NotADirectoryError()
        path = os.path.join(self.cwd, path)
        return self._fs.create_dir(path, recursive=True)

    def exists(self, path: str):
        path = os.path.join(self.cwd, path)
        info = self._fs.get_file_info(path)
        return info.type != FileType.NotFound

    def rename(self, src, dst):
        self._checkfork()
        s = os.path.join(self.cwd, src)
        d = os.path.join(self.cwd, dst)
        return self._fs.move(s, d)

    def remove(self, path, recursive=False):
        delpath = os.path.join(self.cwd, path)
        if self.isdir(path):
            if recursive:
                self._fs.delete_dir(delpath)
            elif list(self.list(path)):
                raise RuntimeError("Directory not empty: {}".format(delpath))
            else:
                self._fs.delete_dir(delpath)
        else:
            self._fs.delete_file(delpath)
