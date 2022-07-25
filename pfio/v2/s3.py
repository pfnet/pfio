import hashlib
import io
import os
from types import TracebackType
from typing import Optional, Type

import boto3
from botocore.exceptions import ClientError

from .fs import FS, FileStat

DEFAULT_MAX_BUFFER_SIZE = 16 * 1024 * 1024


def _normalize_key(key: str) -> str:
    key = os.path.normpath(key)
    if key.startswith("/"):
        return key[1:]
    else:
        return key


class S3ObjectStat(FileStat):
    def __init__(self, key, head):
        self.filename = key
        self.last_modified = head['LastModified'].timestamp()
        self.size = head['ContentLength']
        self.metadata = head['Metadata']
        self._head = head

    def isdir(self):
        return False


class S3PrefixStat(FileStat):
    def __init__(self, key):
        self.filename = key
        self.last_modified = 0
        self.size = -1

    def isdir(self):
        return True


class _ObjectReader(io.RawIOBase):
    def __init__(self, client, bucket, key, mode, kwargs):
        super(_ObjectReader, self).__init__()

        self.client = client
        self.bucket = bucket
        self.key = key

        res = self.client.head_object(Bucket=bucket, Key=key)
        if res.get('DeleteMarker'):
            raise FileNotFoundError()

        self._mode = mode
        self.pos = 0
        self.content_length = res['ContentLength']
        self._closed = False

    def read(self, size=-1) -> bytes:
        # Always returns binary; as this object is wrapped with
        # TextIOWrapper in case of text mode open.

        s = self.pos

        if self.pos >= self.content_length:
            return b''
        elif size <= 0:
            e = ''
        else:
            e = min(self.pos + size, self.content_length)

        r = 'bytes={}-{}'.format(s, e)
        res = self.client.get_object(Bucket=self.bucket,
                                     Key=self.key,
                                     Range=r)
        body = res['Body']

        if size < 0:
            data = body.read()
        else:
            data = body.read(size)

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
                 traceback: Optional[TracebackType]) -> bool:
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


class _ObjectWriter:
    def __init__(self, client, bucket, key, mode, mpu_chunksize, kwargs):
        self.client = client
        self.bucket = bucket
        self.key = key
        self.mode = mode
        self._init_buf()
        self.mpu_chunksize = mpu_chunksize
        self.mpu_id = None
        self.parts = []

    def _init_buf(self):
        if 'b' in self.mode:
            self.buf = io.BytesIO()
        else:
            self.buf = io.StringIO()

    def flush(self):
        # A part must be more than 8 MiB in S3
        if len(self.buf.getvalue()) < 8 * 1024 * 1024:
            return
        self._flush()

    def _flush(self):
        # Send buffer as a part
        c = self.client
        b = self.bucket
        k = self.key

        if self.mpu_id is None:
            res = c.create_multipart_upload(Bucket=b, Key=k)
            self.mpu_id = res['UploadId']
            boto3.set_stream_logger()

        assert self.mpu_id is not None

        data = self.buf.getvalue()
        if 'b' in self.mode:
            md5 = hashlib.md5(data).hexdigest()
        else:
            md5 = hashlib.md5(data.encode()).hexdigest()
        num = len(self.parts) + 1

        res = c.upload_part(Body=data, Bucket=b, Key=k,
                            PartNumber=num,
                            UploadId=self.mpu_id,
                            ContentLength=len(data),
                            ContentMD5=md5)
        self.parts.append({'ETag': res['ETag'], 'PartNumber': num})

        self._init_buf()

    def write(self, buf):
        written = 0
        overflow = len(self.buf.getvalue()) + len(buf) - self.mpu_chunksize
        if overflow > 0:
            l = len(buf) - overflow
            written += self.buf.write(buf[:l])
            self.flush()
            buf = buf[l:]

        written += self.buf.write(buf)
        if len(self.buf.getvalue()) >= self.mpu_chunksize:
            self.flush()

        return written

    def close(self):
        # See:  https://boto3.amazonaws.com/v1/documentation/
        # api/latest/reference/services/s3.html#S3.Client.put_object
        if len(self.parts) == 0:
            self.client.put_object(Body=self.buf.getvalue(),
                                   Bucket=self.bucket,
                                   Key=self.key)
        else:
            self._flush()
            # DO: MPU
            c = self.client
            max_parts = len(self.parts) + 1
            res = c.list_parts(Bucket=self.bucket,
                               Key=self.key,
                               UploadId=self.mpu_id, MaxParts=max_parts)

            if res['IsTruncated']:
                next_part = res['NextPartNumberMarker']
                raise RuntimeError('Unexpectedly truncated: ' +
                                   'next={}/maxparts={}'.format(next_part,
                                                                max_parts))

            parts = [{'ETag': part['ETag'], 'PartNumber': part['PartNumber']}
                     for part in res.get('Parts', [])]
            parts = sorted(parts, key=lambda x: int(x['PartNumber']))
            assert self.parts == parts

            res = c.complete_multipart_upload(Bucket=self.bucket,
                                              Key=self.key,
                                              UploadId=self.mpu_id,
                                              MultipartUpload={'Parts': parts})
            # logger.info("Upload done.", res['Location'])

        self.buf = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.close()

    @property
    def closed(self):
        return self.buf is None

    def isatty(self):
        return False

    def readable(self):
        return False

    def seekable(self):
        return False

    def writable(self):
        return True


class S3(FS):
    '''S3 FileSystem wrapper

    Takes three arguments as well as enviroment variables for
    constructor. The priority is (1) see arguments, (2) see enviroment
    variables, (3) take boto3's default. Available arguments are:

    - ``aws_access_key_id``, ``AWS_ACCESS_KEY_ID``
    - ``aws_secret_access_key``, ``AWS_SECRET_ACCESS_KEY``
    - ``endpoint``, ``S3_ENDPOINT``

    It supports buffering when opening a file in binary read mode ("rb").
    When ``buffering`` is set to -1 (default), the buffer size will be
    the size of the file or ``pfio.v2.S3.DEFAULT_MAX_BUFFER_SIZE``,
    whichever smaller.
    ``buffering=0`` disables buffering, and ``buffering>0`` forcibly sets the
    specified value as the buffer size in bytes.
    '''

    def __init__(self, bucket, prefix=None,
                 endpoint=None, create_bucket=False,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 mpu_chunksize=32*1024*1024,
                 buffering=-1,
                 create=False,
                 reset_on_fork=False,
                 **_):
        super().__init__(reset_on_fork=reset_on_fork)
        self.bucket = bucket
        self.create_bucket = create_bucket
        if prefix is not None:
            self.cwd = prefix
        else:
            self.cwd = ''

        # In S3, create flag can be disregarded
        del create

        self.mpu_chunksize = mpu_chunksize
        self.buffering = buffering

        # boto3.set_stream_logger()

        # import botocore
        # botocore.session.Session().set_debug_logger()

        kwargs = {}

        # IF these arguments are not defined, the library
        # automatically retrieves from AWS_ACCESS_KEY_ID and
        # AWS_SECRET_ACCESS_KEY.
        self.aws_access_key_id = aws_access_key_id
        if aws_access_key_id is not None:
            kwargs['aws_access_key_id'] = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        if aws_secret_access_key is not None:
            kwargs['aws_secret_access_key'] = aws_secret_access_key

        # We won't expect any enviroment variable for S3 endpoints
        # supported by boto3. Instead, we take S3_ENDPOINT in case
        # argument ``endpoint`` is not given. Otherwise, it goes to
        # boto3's default by giving ``None``.
        #
        # See also:
        # https://github.com/boto/boto3/issues/1375
        # https://github.com/boto/boto3/pull/2746
        self.endpoint = endpoint
        if self.endpoint is None:
            self.endpoint = os.getenv('S3_ENDPOINT')
        if self.endpoint is not None:
            kwargs['endpoint_url'] = self.endpoint

        self.kwargs = kwargs
        self._connect()

    def _reset(self):
        self._connect()

    def _connect(self):
        # print('boto3.client options:', kwargs)
        self.client = boto3.client('s3', **self.kwargs)

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404' and self.create_bucket:
                res = self.client.create_bucket(Bucket=self.bucket)
                print("Bucket", self.bucket, "created:", res)
            else:
                raise e

    def __getstate__(self):
        state = self.__dict__.copy()
        state['client'] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state

    def open(self, path, mode='r', **kwargs):
        '''Opens an object accessor for read or write

        .. note:: Multi-part upload is not yet available.

        Arguments:
            path (str): relative path from basedir

            mode (str): open mode
        '''
        self._checkfork()
        if 'a' in mode:
            raise io.UnsupportedOperation('Append is not supported')
        if 'r' in mode and 'w' in mode:
            raise io.UnsupportedOperation('Read-write mode is not supported')

        path = os.path.join(self.cwd, path)
        path = _normalize_key(path)
        if 'r' in mode:
            obj = _ObjectReader(self.client, self.bucket, path, mode, kwargs)

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
            obj = _ObjectWriter(self.client, self.bucket, path, mode,
                                self.mpu_chunksize, kwargs)
            if 'b' in mode:
                obj = io.BufferedWriter(obj)

        else:
            raise RuntimeError(f'Unknown option: {mode}')

        return obj

    def list(self, prefix: str = "", recursive=False):
        '''List all objects (and prefixes)

        Although there is not concept of directory in AWS S3 API,
        common prefixes shows up like directories.

        '''
        self._checkfork()
        key = os.path.join(self.cwd, prefix)
        key = _normalize_key(key)
        if key == '.':
            key = ''
        elif key != '' and not key.endswith('/'):
            key += '/'
        if '/../' in key or key.startswith('..'):
            raise ValueError('Invalid S3 key: {} as {}'.format(prefix, key))

        page_size = 1000
        paginator = self.client.get_paginator('list_objects_v2')
        paging_args = {
            'Bucket': self.bucket, 'Prefix': key,
            'PaginationConfig': {'PageSize': page_size}
        }
        if not recursive:
            paging_args['Delimiter'] = '/'

        iterator = paginator.paginate(**paging_args)
        for res in iterator:
            # print(res)
            for common_prefix in res.get('CommonPrefixes', []):
                yield common_prefix['Prefix'][len(key):]
            for content in res.get('Contents', []):
                yield content['Key'][len(key):]

    def stat(self, path):
        '''Imitate FileStat with S3 Object metadata

        '''
        self._checkfork()
        key = os.path.join(self.cwd, path)
        key = _normalize_key(key)
        try:
            res = self.client.head_object(Bucket=self.bucket,
                                          Key=key)
            if res.get('DeleteMarker'):
                raise FileNotFoundError()

            return S3ObjectStat(key, res)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                if self.isdir(path):
                    return S3PrefixStat(key)
                raise FileNotFoundError()
            else:
                raise e

    def isdir(self, file_path: str):
        '''Imitate isdir by handling common prefix ending with "/" as directory

        AWS S3 does not have concept of directory tree, but this class
        imitates other file systems to increase compatibility.
        '''
        self._checkfork()
        key = _normalize_key(os.path.join(self.cwd, file_path))
        if key == '.':
            key = ''
        elif key.endswith('/'):
            key = key[:-1]
        if '/../' in key or key.startswith('..'):
            raise ValueError('Invalid S3 key: {} as {}'.format(file_path, key))

        if len(key) == 0:
            return True

        res = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=key,
            Delimiter="/",
            MaxKeys=1,
        )
        for common_prefix in res.get('CommonPrefixes', []):
            if common_prefix['Prefix'] == key + "/":
                return True
        return False

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        '''Does nothing

        .. note:: AWS S3 does not have concept of directory tree; what
           this function (and ``makedirs()``) should do
           and return? To be strict, it would be straightforward to
           raise ``io.UnsupportedOperation`` exception. But it just
           breaks users' applications that except quasi-compatible
           behaviour. Thus, imitating other file systems, like
           returning ``None`` would be nicer.
        '''
        # raise io.UnsupportedOperation("S3 doesn't have directory")
        pass

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        '''Does nothing

        .. note:: see discussion in ``mkdir()``.
        '''
        # raise io.UnsupportedOperation("S3 doesn't have directory")
        pass

    def exists(self, file_path: str):
        '''Returns the existence of objects

        For common prefixes, it does nothing. See discussion in ``isdir()``.
        '''
        self._checkfork()
        try:
            key = os.path.join(self.cwd, file_path)
            key = _normalize_key(key)
            res = self.client.head_object(Bucket=self.bucket,
                                          Key=key)
            return not res.get('DeleteMarker')
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                if self.isdir(file_path):
                    return True
                return False
            else:
                raise e

    def rename(self, src, dst):
        '''Copies & removes the object

        Source and destination must be in the same bucket for
        ``pfio``, although AWS S3 supports inter-bucket copying.

        '''
        self._checkfork()
        source = {
            'Bucket': self.bucket,
            'Key': _normalize_key(os.path.join(self.cwd, src)),
        }
        dst = os.path.join(self.cwd, dst)
        dst = _normalize_key(dst)
        res = self.client.copy_object(Bucket=self.bucket,
                                      CopySource=source,
                                      Key=dst)
        if not res.get('CopyObjectResult'):
            # copy failed
            return
        return self.remove(src)

    def remove(self, file_path: str, recursive=False):
        '''Removes an object

        It raises a FileNotFoundError when the specified file doesn't exist.
        '''
        if recursive:
            raise io.UnsupportedOperation("Recursive delete not supported")

        if not self.exists(file_path):
            msg = "No such S3 object: '{}'".format(file_path)
            raise FileNotFoundError(msg)

        self._checkfork()
        key = os.path.join(self.cwd, file_path)
        key = _normalize_key(key)
        return self.client.delete_object(Bucket=self.bucket,
                                         Key=key)
