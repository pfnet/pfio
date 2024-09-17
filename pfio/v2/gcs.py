import io
import json
import os
from types import TracebackType
from typing import Optional, Type

import google.cloud.storage as storage
from google.cloud import exceptions
from google.cloud.storage.fileio import BlobWriter
from google.oauth2 import service_account

from ._profiler import record, record_iterable
from .fs import FS, FileStat

DEFAULT_MAX_BUFFER_SIZE = 16 * 1024 * 1024


def _normalize_key(key: str) -> str:
    key = os.path.normpath(key)
    if key.startswith("/"):
        return key[1:]
    else:
        return key


def _format_path(abspath, prefix):
    """ Absolute paths returned by gcs.list are converted
        to s3-compatible representation
    """
    return abspath[len(prefix):]


class GCSProfileIOWrapper:
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        self.obj.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with record("pfio.v2.GoogleCloudStorage:exit-context", trace=True):
            self.obj.__exit__(exc_type, exc_value, traceback)

    def __getattr__(self, name):
        attr = getattr(self.obj, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                with record(f"pfio.v2.GoogleCloudStorage:{attr.__name__}",
                            trace=True):
                    return attr(*args, **kwargs)

            return wrapper
        else:
            return attr


class ObjectStat(FileStat):
    def __init__(self, blob, path):
        self.filename = path
        self.last_modified = blob.updated
        self.size = blob.size
        self.metadata = blob.metadata
        self._head = blob

    def isdir(self):
        return self.size == 0 and self.path.endswith('/')


class PrefixStat(FileStat):
    def __init__(self, key, path):
        self.filename = path
        self.last_modified = 0
        self.size = -1

    def isdir(self):
        return True


class _ObjectReader(io.RawIOBase):
    def __init__(self, blob):
        super(_ObjectReader, self).__init__()

        self.blob = blob
        self.pos = 0
        self.content_length = blob.size
        self._closed = False

    def read(self, size=-1) -> bytes:
        # Always returns binary; as this object is wrapped with
        # TextIOWrapper in case of text mode open.

        s = self.pos

        if self.pos >= self.content_length:
            return b''
        elif size <= 0:
            e = None
        else:
            e = min(self.pos + size, self.content_length)

        body = self.blob.download_as_bytes(start=s, end=e)

        if size < 0:
            data = body
        else:
            data = body[:size]

        self.pos += len(data)

        return data

    def readline(self):
        raise NotImplementedError()

    def close(self):
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]):
        self.close()

    def flush(self):
        pass

    @property
    def closed(self):
        return self._closed

    def isatty(self):
        return False

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def truncate(self, size=None):
        raise io.UnsupportedOperation('truncate')

    def seek(self, pos, whence=io.SEEK_SET):
        if whence in [0, io.SEEK_SET]:
            if pos < 0:
                raise OSError(22, "[Errno 22] Invalid argument")
        elif whence in [1, io.SEEK_CUR]:
            pos += self.pos
        elif whence in [2, io.SEEK_END]:
            pos += self.content_length
        else:
            raise ValueError('Wrong whence value: {}'.format(whence))

        if pos < 0:
            raise OSError(22, "[Errno 22] Invalid argument")
        self.pos = pos
        return self.pos

    def writable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('not writable')

    def readall(self):
        return self.read(-1)

    def readinto(self, b):
        buf = self.read(len(b))
        b[:len(buf)] = buf
        return len(buf)


class GoogleCloudStorage(FS):
    """Google Cloud Storage wrapper

    ``key_path`` argument is a path to credential files of
    IAM service account. The path to the file can be set to
    the environmental variable``GOOGLE_APPLICATION_CREDENTIALS``
    instead.

    .. note:: This is an experimental implmentation.

    """

    def __init__(self, bucket: str, prefix=None,
                 key_path=None,
                 create_bucket=False,
                 mpu_chunksize=32 * 1024 * 1024,
                 buffering=-1,
                 create=False,
                 connect_timeout=None,
                 ignore_flush=False,
                 trace=False,
                 **_):
        super().__init__()
        self.bucket_name = bucket
        self.key_path = key_path

        if prefix is not None:
            self.cwd = prefix
        else:
            self.cwd = ''

        # In GCS, create flag can be disregarded
        del create

        self.buffering = buffering
        self.trace = trace
        self.connect_time = connect_timeout
        self.create_bucket = create_bucket
        self.ignore_flush = ignore_flush

        self._reset()

    def _reset(self):
        if self.key_path is None:
            self.client = storage.Client()
        else:
            with open(self.key_path) as kp:
                service_account_info = json.load(kp)
                credentials = service_account.Credentials. \
                    from_service_account_info(service_account_info)
            self.client = storage.Client(credentials=credentials,
                                         project=credentials.project_id)

        # Caveat: You'll need
        # ``roles/storage.insightsCollectorService`` role for the
        # accessor instead.  This is because
        # ``roles/storage.objectViewer`` does not have
        # ``storage.buckets.get`` which is needed to call
        # ``get_bucket()``.
        #
        # See also:
        # https://cloud.google.com/storage/docs/access-control/iam-roles

        try:
            self.bucket = self.client.get_bucket(
                self.bucket_name, timeout=self.connect_time)
        except exceptions.NotFound as e:
            if self.create_bucket:
                self.bucket = self.client.create_bucket(self.bucket_name)
                print("Bucket", self.bucket_name, "created:", self.bucket)
            else:
                raise e

        assert self.bucket

    def __getstate__(self):
        state = self.__dict__.copy()
        state['client'] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state

    def open(self, path, mode='r', **kwargs):
        """Opens an object accessor for read or write

        .. note:: Multi-part upload is not yet available.

        Arguments:
            path (str): relative path from basedir

            mode (str): open mode
        """
        with record("pfio.v2.GoogleCloudStorage:open", trace=self.trace):
            self._checkfork()
            if 'a' in mode:
                raise io.UnsupportedOperation('Append is not supported')
            if 'r' in mode and 'w' in mode:
                raise io.UnsupportedOperation(
                    'Read-write mode is not supported'
                )

            path = os.path.join(self.cwd, path)
            blob = self.bucket.get_blob(path, timeout=self.connect_time)
            if blob is None:
                blob = self.bucket.blob(path)

            if 'r' in mode:
                obj = _ObjectReader(blob)

                bs = self.buffering
                if bs < 0:
                    bs = min(obj.content_length, DEFAULT_MAX_BUFFER_SIZE)

                if 'b' in mode:
                    if self.buffering and bs != 0:
                        obj = io.BufferedReader(obj, buffer_size=bs)
                else:
                    obj = io.TextIOWrapper(obj)
                    if self.buffering:
                        # This is undocumented property; but resident at
                        # least since 2009 (the merge of io-c branch).
                        # We'll use it until the day of removal.
                        if bs == 0:
                            # empty file case: _CHUNK_SIZE must be positive
                            bs = DEFAULT_MAX_BUFFER_SIZE
                        obj._CHUNK_SIZE = bs

            elif 'w' in mode:
                # Create intermediate directories as simulated ones
                ps = path.split('/')
                xdi = -1  # xdi means `reverse order index`
                while len(ps[:xdi]) != 0:
                    self.__make_simulated_dir('/'.join(ps[:xdi]))
                    xdi -= 1
                if 'b' in mode:
                    obj = BlobWriter(blob, chunk_size=1024 * 1024,
                                     ignore_flush=self.ignore_flush)
                else:
                    obj = io.TextIOWrapper(
                        BlobWriter(blob, chunk_size=1024 * 1024,
                                   ignore_flush=True))
            else:
                raise RuntimeError(f'Unknown option: {mode}')

            if self.trace:
                return GCSProfileIOWrapper(obj)
            else:
                return obj

    def list(self, prefix: Optional[str] = "", recursive=False, detail=False):
        """List all objects (and prefixes)

        Although there is not concept of directory in GCS API,
        common prefixes shows up like directories.
        """
        for e in record_iterable("pfio.v2.GoogleCloudStorage:list",
                                 self._list(prefix, recursive, detail),
                                 trace=self.trace):
            yield e

    def _list(self, prefix: Optional[str] = "", recursive=False, detail=False):
        path = os.path.join(self.cwd, "" if prefix is None else prefix)
        path = _normalize_key(path)
        if path == '.':
            path = ''
        elif path != '' and not path.endswith('/'):
            path += '/'
        if '/../' in path or path.startswith('..'):
            raise ValueError('Invalid GCS key: {} as {}'.format(prefix, path))

        blobs = self.bucket.list_blobs(prefix=path,
                                       delimiter=('' if recursive else '/'),
                                       timeout=self.connect_time)
        # objects
        for blob in blobs:
            if blob.name == path:
                continue

            if detail:
                yield ObjectStat(blob, _format_path(blob.name, path))
            else:
                yield _format_path(blob.name, path)
        # folders
        for prefix in blobs.prefixes:
            if detail:
                yield PrefixStat(_format_path(prefix, path))
            else:
                yield _format_path(prefix, path)

    def stat(self, path):
        """Imitate FileStat with S3 Object metadata

        """
        with record("pfio.v2.GoogleCloudStorage:stat", trace=self.trace):
            self._checkfork()
            path = _normalize_key(os.path.join(self.cwd, path))

            blob = self.bucket.get_blob(path)
            return ObjectStat(blob, self._get_relative_path(blob.name))

    def isdir(self, file_path):
        """
        Imitate isdir by handling common prefix ending with "/" as directory

        GoogleCloudStorage does not have concept of directory tree,
        but this classimitates other file systems to increase compatibility.
        """

        with record("pfio.v2.GoogleCloudStorage:isdir", trace=self.trace):
            self._checkfork()

            path = _normalize_key(os.path.join(self.cwd, file_path))
            if path == '.':
                path = ''
            elif path != '' and not path.endswith('/'):
                path += '/'
            if '/../' in path or path.startswith('..'):
                raise ValueError(f'Invalid GCS key: {file_path} as {path}')

            if len(path) == 0:
                return True

            # get a parent folder
            tmp = path.rsplit('/', 2)
            parent_dir = tmp[0] if len(tmp) > 2 else ''
            parent_dir += '/' if len(parent_dir) > 0 else ''

            blobs = self.bucket.list_blobs(prefix=parent_dir, delimiter='/',
                                           timeout=self.connect_time)
            list(blobs)
            return path in blobs.prefixes

    def __make_simulated_dir(self, object_name: str) -> None:
        """Make a simulated folder

        Args:
            object_name (str): Should end with '/'

        Returns:
            None

        """
        # Ensure the object name for a simulated folder ends with '/' letter.
        if not object_name.endswith('/'):
            object_name += '/'

        blob = self.bucket.blob(object_name)
        if not blob.exists(timeout=self.connect_time):
            blob.upload_from_string('',
                                    content_type='application/octet-stream',
                                    timeout=self.connect_time)

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        """Make a simulated folder

        Args:
            path (str): the path to the directory to make

        .. note:: GCS does not have concept of directory tree; but
           some tools simulate folders.
           Follow the behavior of creating a zero-byte objects as
           folder placeholders, such as Google Cloud console.

        """
        with record("pfio.v2.GoogleCloudStorage:mkdir", trace=self.trace):
            self._checkfork()
            self.__make_simulated_dir(
                _normalize_key(os.path.join(self.cwd, file_path)) + '/')

    def makedirs(self, file_path: str, mode: int = 0o777,
                 exist_ok: bool = False) -> None:
        """Make simulated folders recursively

        Creates all the missing parents of the given path.

        Args:
            path (str): the path to the directory to make.

        .. note: GCS does not have concept of directory tree; but
           some tools simulate folders.
           Follow the behavior of creating a zero-byte objects as
           folder placeholders, such as Google Cloud console.

        """
        with record("pfio.v2.GoogleCloudStorage:makedirs", trace=self.trace):
            self._checkfork()
            target_path = _normalize_key(os.path.join(self.cwd, file_path))
            object_name, *tail = target_path.split('/')
            object_name += '/'
            self.__make_simulated_dir(object_name)
            for part in tail:
                object_name = f'{object_name}{part}/'
                self.__make_simulated_dir(object_name)

    def exists(self, path: str) -> bool:
        """Returns whether an object exists or not
        """
        with record("pfio.v2.GoogleCloudStorage:exists", trace=self.trace):
            self._checkfork()
            object_name = _normalize_key(os.path.join(self.cwd, path))

            if object_name == "":
                return self.bucket.exists()

            return \
                self.bucket.blob(object_name)\
                    .exists(timeout=self.connect_time) or \
                self.bucket.blob(object_name + '/')\
                    .exists(timeout=self.connect_time)

    def rename(self, src, dst):
        """Copies & removes the object

        Source and destination must be in the same bucket for
        ``pfio``, although GCS supports inter-bucket copying.

        """
        with record("pfio.v2.GoogleCloudStorage:rename", trace=self.trace):
            self._checkfork()
            src = self.cwd + "/" + src
            dst = self.cwd + "/" + dst

            source_blob = self.bucket.blob(src)

            # Returns Blob destination
            self.bucket.copy_blob(source_blob,
                                  self.bucket,
                                  new_name=dst,
                                  timeout=self.connect_time)
            return self.bucket.delete_blob(src, timeout=self.connect_time)

    def remove(self, path: str, recursive=False) -> None:
        """Removes an object

        It raises a FileNotFoundError when the specified file doesn't exist.
        """
        with record("pfio.v2.GoogleCloudStorage:remove", trace=self.trace):
            self._checkfork()

            object_name = _normalize_key(os.path.join(self.cwd, path))
            exists_as_folder = self.bucket \
                .blob(object_name + '/') \
                .exists(timeout=self.connect_time)
            exists_as_file = self.bucket \
                .blob(object_name) \
                .exists(timeout=self.connect_time)

            if not exists_as_folder and not exists_as_file:
                msg = f"No such GCS object: '{path}'"
                raise FileNotFoundError(msg)

            if exists_as_file:
                self.bucket.delete_blob(object_name, timeout=self.connect_time)
                return

            if exists_as_folder:
                if not recursive:
                    raise io.UnsupportedOperation("Please add recursive=True \
                                                  to remove a directory")
                blobs = self.bucket.list_blobs(prefix=object_name + '/',
                                               delimiter='',
                                               timeout=self.connect_time)
                for blob in blobs:
                    self.bucket.delete_blob(blob.name,
                                            timeout=self.connect_time)
                for prefix in blobs.prefixes:
                    self.bucket.delete_blob(prefix.name,
                                            timeout=self.connect_time)

    def _canonical_name(self, file_path: str) -> str:
        path = os.path.join(self.cwd, file_path)
        norm_path = _normalize_key(path)

        return f"gs://{self.bucket_name}/{norm_path}"
