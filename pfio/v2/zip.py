import io
import logging
import os
import zipfile
from datetime import datetime

from pfio.cache.sparse_file import CachedWrapper, MPCachedWrapper

from .fs import FS, FileStat

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class ZipFileStat(FileStat):
    """Detailed information of a file in a Zip

    Attributes:
        filename (str): Derived from `~FileStat`.
        orig_filename (str): ``ZipFile.orig_filename``.
        comment (str): ``ZipFile.comment``.
        last_modifled (float): Derived from `~FileStat`.
            No sub-second precision.
        mode (int): Derived from `~FileStat`.
        size (int): Derived from `~FileStat`.
        create_system (int): ``ZipFile.create_system``.
        create_version (int): ``ZipFile.create_version``.
        extract_version (int): ``ZipFile.extract_version``.
        flag_bits (int): ``ZipFile.flag_bits``.
        volume (int): ``ZipFile.volume``.
        internal_attr (int): ``ZipFile.internal_attr``.
        external_attr (int): ``ZipFile.external_attr``.
        header_offset (int): ``ZipFile.header_offset``.
        compress_size (int): ``ZipFile.compress_size``.
        compress_type (int): ``ZipFile.compress_type``.
        CRC (int): ``ZipFile.CRC``.
    """

    def __init__(self, zip_info):
        self.last_modified = float(datetime(*zip_info.date_time).timestamp())
        # https://github.com/python/cpython/blob/3.8/Lib/zipfile.py#L392
        self.mode = zip_info.external_attr >> 16
        self.size = zip_info.file_size

        for k in ('filename', 'orig_filename', 'comment', 'create_system',
                  'create_version', 'extract_version', 'flag_bits',
                  'volume', 'internal_attr', 'external_attr', 'CRC',
                  'header_offset', 'compress_size', 'compress_type'):
            setattr(self, k, getattr(zip_info, k))


class Zip(FS):
    _readonly = True
    '''
            local_cache (bool): use sparse file cache for opening ZIP file

            local_cachedir (dir): local path to store sparse file cache
    '''

    def __init__(self, backend, file_path, mode='r', create=False,
                 local_cache=False, local_cachedir=None,
                 reset_on_fork=False, **kwargs):
        super().__init__(reset_on_fork=reset_on_fork)
        self.backend = backend
        self.file_path = file_path
        self.mode = mode
        self.kwargs = kwargs

        if create:
            raise ValueError("create option is not supported")

        if 'r' in mode and 'w' in mode:
            raise io.UnsupportedOperation('Read-write mode is not supported')

        if 'w' in mode:
            self._readonly = False

        self.local_cache = local_cache
        self.local_cachedir = local_cachedir
        self.local_cachefile = None
        self.local_indexfile = None

        self._reset()

    def _reset(self):
        buffering = self.kwargs.get('buffering', 16*1024*1024)

        if self.local_cache:
            # Don't use io.BufferedReader for sparse file cache
            self.kwargs['buffering'] = 0

        obj = self.backend.open(self.file_path,
                                self.mode + 'b',
                                **self.kwargs)

        stat = self.backend.stat(self.file_path)

        # Use sparse file cache: Optimization for a remote object
        # store system e.g. AWS S3 or HDFS
        if self.local_cache:
            self.kwargs['buffering'] = buffering

            if self.reset_on_fork:
                obj = MPCachedWrapper(obj, stat.size, self.local_cachedir,
                                      local_cachefile=self.local_cachefile,
                                      local_indexfile=self.local_indexfile,
                                      close_on_close=True,
                                      multithread_safe=True)

                # Update local cachefile in case of being forked
                self.local_cachefile = obj.local_cachefile
                self.local_indexfile = obj.local_indexfile
            else:
                obj = CachedWrapper(obj, stat.size, self.local_cachedir,
                                    close_on_close=True, multithread_safe=True)

                # Default 16MB buffer size
            if buffering > 0:
                obj = io.BufferedReader(obj, buffer_size=buffering)

        self.fileobj = obj

        assert self.fileobj is not None
        self.zipobj = zipfile.ZipFile(self.fileobj, self.mode)

    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):
        self._checkfork()

        file_path = os.path.join(self.cwd, os.path.normpath(file_path))
        fp = self.zipobj.open(file_path, mode.replace('b', ''))

        if 'b' not in mode:
            fp = io.TextIOWrapper(fp, encoding, errors, newline)

        return fp

    def subfs(self, path):
        # TODO
        raise NotImplementedError()

    def close(self):
        self._checkfork()
        self.zipobj.close()
        self.fileobj.close()

    def stat(self, path):
        self._checkfork()
        path = os.path.join(self.cwd, os.path.normpath(path))
        if path in self.zipobj.namelist():
            actual_path = path
        elif (not path.endswith('/')
              and path + '/' in self.zipobj.namelist()):
            # handles cases when path is a directory but without trailing slash
            # see issue $67
            actual_path = path + '/'
        else:
            raise FileNotFoundError(
                "{} is not found".format(path))

        return ZipFileStat(self.zipobj.getinfo(actual_path))

    def list(self, path_or_prefix: str = "", recursive=False):
        self._checkfork()

        if path_or_prefix:
            path_or_prefix = os.path.join(self.cwd,
                                          os.path.normpath(path_or_prefix))
            # cannot move beyond root
            given_dir_list = path_or_prefix.split('/')
            if ("." in given_dir_list or ".." in given_dir_list
                    or {""} == set(given_dir_list)):
                given_dir_list = []
                path_or_prefix = ""
        else:
            given_dir_list = []

        if path_or_prefix:
            if self.exists(path_or_prefix) and not self.isdir(path_or_prefix):
                raise NotADirectoryError(
                    "{} is not a directory".format(path_or_prefix))
            elif not any(name.startswith(path_or_prefix + "/")
                         for name in self.zipobj.namelist()):
                # check if directories are NOT included in the zip
                # such kind of zip can be made with "zip -D"
                raise FileNotFoundError(
                    "{} is not found".format(path_or_prefix))

        if recursive:
            for name in self.zipobj.namelist():
                if name.startswith(path_or_prefix):
                    name = name[len(path_or_prefix):].strip("/")
                    if name:
                        yield name
        else:
            _list = set()
            for name in self.zipobj.namelist():
                return_file_name = None
                current_dir_list = os.path.normpath(name).split('/')
                if not given_dir_list:
                    # if path_or_prefix is not given
                    return_file_name = current_dir_list[0]
                else:
                    if (current_dir_list
                            and len(current_dir_list) > len(given_dir_list)
                            and current_dir_list[:len(given_dir_list)] ==
                            given_dir_list):
                        return_file_name = current_dir_list[
                            len(given_dir_list):][0]

                if (return_file_name is not None
                        and return_file_name not in _list):
                    _list.add(return_file_name)
                    yield return_file_name

    def isdir(self, file_path: str):
        self._checkfork()
        file_path = os.path.join(self.cwd, file_path)
        if self.exists(file_path):
            return self.stat(file_path).isdir()
        else:
            file_path = os.path.normpath(file_path)
            # check if directories are NOT included in the zip
            if any(name.startswith(file_path + "/")
                   for name in self.zipobj.namelist()):
                return True

            return False

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        raise io.UnsupportedOperation("zip does not support mkdir")

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        raise io.UnsupportedOperation("zip does not support makedirs")

    def exists(self, file_path: str):
        self._checkfork()
        file_path = os.path.join(self.cwd, os.path.normpath(file_path))
        namelist = self.zipobj.namelist()
        return (file_path in namelist
                or file_path + "/" in namelist)

    def rename(self, *args):
        raise io.UnsupportedOperation

    def remove(self, file_path, recursive=False):
        raise io.UnsupportedOperation
