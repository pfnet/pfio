from chainerio.container import Container
from chainerio.fileobject import FileObject
from chainerio.io import open_wrapper
import warnings
import io
import logging
import os
import zipfile
import sys

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class ZipFileObject(FileObject):
    def __init__(self, base_file_object, base_filesystem_handler,
                 io_profiler, path: str, mode: str = "r", buffering=-1,
                 encoding=None, errors=None, newline=None,
                 closefd=True, opener=None):
        if 'b' not in mode:
            base_file_object = io.TextIOWrapper(base_file_object,
                                                encoding, errors, newline)
        elif sys.version_info < (3, 7, ):
            # In the old implemetaion of zipfile before Python 3.7,
            # the ZipExtFile was not seekable, which makes nested zip
            # difficult since making a zip requires the file to be seekable.
            # As a workaround we put the data into BytesIO object.

            warnings.warn('In the current Python, Chainerio has to read '
                          'the whole file content from the zip '
                          'on open, which might cause performance or '
                          'memory issues. '
                          'Use Python >= 3.7 to avoid.',
                           RuntimeWarning)

            base_file_object = io.BytesIO(base_file_object.read())

        FileObject.__init__(self, base_file_object, base_filesystem_handler,
                            path, mode, buffering, encoding, errors, newline,
                            closefd, opener)


class ZipContainer(Container):
    def __init__(self, base_handler, base):
        Container.__init__(self, base_handler, base)
        self._check_zip_file_name(base)

        logger.info("using zip container for {}".format(base))
        self.zip_file_obj = None
        self.type = "zip"
        self.fileobj_class = ZipFileObject

    def _check_zip_file_name(self, base):
        assert not "" == base and None is not base,\
            "No zip base file assigned"
        filename, file_extension = os.path.splitext(self.base)

    def _open_zip_file(self, mode='r'):
        mode = mode.replace("b", "")
        if self.zip_file_obj is None:
            zip_file = self.base_handler.open(self.base, "rb")
            self.zip_file_obj = zipfile.ZipFile(zip_file, mode)

    def _close_zip_file(self):
        if None is not self.zip_file_obj:
            self.zip_file_obj.close()
            self.zip_file_obj = None

    @open_wrapper
    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):

        self._open_zip_file(mode)

        # zip only supports open with r rU or U
        nested_file = self.zip_file_obj.open(file_path, "r")
        return nested_file

    def close(self):
        self._close_zip_file()

    def info(self):
        info_str = \
            "this is zip container with filename {} on {} filesystem".format(
                self.base, self.base_handler.type)
        return info_str

    def stat(self, path):
        self._open_zip_file()
        return self.zip_file_obj.getinfo(path)

    def list(self, path_or_prefix: str = None):
        self._open_zip_file()
        return self._list(path_or_prefix)

    def _list(self, path_or_prefix: str = None):
        _list = set()
        for name in self.zip_file_obj.namelist():
            if path_or_prefix and name.startswith(path_or_prefix):
                name = name[len(path_or_prefix):]

            first_level_file_name = name.split("/")[0]
            if first_level_file_name and first_level_file_name not in _list:
                _list.add(first_level_file_name)
                yield first_level_file_name

    def set_base(self, base):
        Container.reset_base_handler(self, base)

        if None is not self.tar_file_obj:
            self.zip_file_obj.close()
            self.zip_file_obj = None

    def isdir(self, file_path: str):
        stat = self.stat(file_path)
        # The `is_dir` function under `ZipInfo` object
        # is not available on my testbed
        # Copied the code from the `zipfile.py`
        return "/" == stat.filename[-1]

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        raise io.UnsupportedOperation("zip does not support mkdir")

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        raise io.UnsupportedOperation("zip does not support makedirs")

    def exists(self, file_path: str):
        self._open_zip_file()
        return file_path in self.zip_file_obj.namelist()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if None is not self.zip_file_obj:
            self.zip_file_obj.close()
            self.zip_file_obj = None

    def remove(self, file_path, recursive=False):
        raise io.UnsupportedOperation
