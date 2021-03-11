import io
import os
from types import TracebackType
from typing import Optional, Type

import boto3
from botocore.exceptions import ClientError

from .fs import FS, FileStat


class S3ObjectStat(FileStat):
    def __init__(self, key, head):
        self.filename = key
        self.last_modified = head['LastModified'].timestamp()
        self.size = head['ContentLength']
        self.metadata = head['Metadata']
        self._head = head

    def isdir(self):
        return False


class _ObjectReader:
    def __init__(self, client, bucket, key, mode, kwargs):
        self.client = client
        self.res = self.client.get_object(Bucket=bucket,
                                          Key=key)
        self._mode = mode
        self.body = self.res['Body']
        self.pos = 0
        self.content_length = self.res['ContentLength']

    def read(self, size=-1):
        if size <= 0:
            size = None

        if self.content_length <= self.pos:
            return

        if 'b' in self._mode:
            data = self.body.read(size)
        else:
            data = self.body.read(size).decode('utf-8')

        self.pos += len(data)
        return data

    def close(self):
        self.body = None

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
        return self.body is None

    def isatty(self):
        return False

    def readable(self):
        return True

    def seekable(self):
        return False

    def writable(self):
        return False


class _ObjectWriter:
    def __init__(self, client, bucket, key, mode, kwargs):
        self.client = client
        self.bucket = bucket
        self.key = key
        if 'b' in mode:
            self.buf = io.BytesIO()
        else:
            self.buf = io.StringIO()

    def write(self, buf):
        return self.buf.write(buf)

    def close(self):
        # TODO: MPU
        # See:  https://boto3.amazonaws.com/v1/documentation/
        # api/latest/reference/services/s3.html#S3.Client.put_object
        self.client.put_object(Body=self.buf.getvalue(),
                               Bucket=self.bucket,
                               Key=self.key)
        self.buf = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.close()

    def flush(self):
        '''Does nothing
        '''
        pass

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

    '''

    def __init__(self, bucket, prefix=None,
                 endpoint=None, create_bucket=False,
                 aws_access_key_id=None,
                 aws_secret_access_key=None):
        self.bucket = bucket
        if prefix is not None:
            self.cwd = prefix
        else:
            self.cwd = ''

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

        # print('boto3.client options:', kwargs)
        self.client = boto3.client('s3', **kwargs)

        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404' and create_bucket:
                res = self.client.create_bucket(Bucket=bucket)
                print("Bucket", bucket, "created:", res)
            else:
                raise e

    def open(self, path, mode='r', **kwargs):
        '''Opens an object accessor for read or write

        .. note:: Multi-part upload is not yet available.

        '''
        if 'a' in mode:
            raise io.UnsupportedOperation('Append is not supported')
        if 'r' in mode and 'w' in mode:
            raise io.UnsupportedOperation('Read-write mode is not supported')

        path = os.path.join(self.cwd, path)
        if 'r' in mode:
            obj = _ObjectReader(self.client, self.bucket, path, mode, kwargs)
            if 'b' in mode:
                obj = io.BufferedReader(obj)

        elif 'w' in mode:
            obj = _ObjectWriter(self.client, self.bucket, path, mode, kwargs)
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
        key = os.path.normpath(os.path.join(self.cwd, prefix))
        if key == '.':
            key = ''
        if key:
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
                yield common_prefix['Prefix']
            for content in res.get('Contents', []):
                yield content['Key'][len(key):]

    def stat(self, path):
        '''Imitate FileStat with S3 Object metadata

        '''
        self._checkfork()
        key = os.path.join(self.cwd, path)
        res = self.client.head_object(Bucket=self.bucket,
                                      Key=key)
        if res.get('DeleteMarker'):
            raise FileNotFoundError()

        return S3ObjectStat(key, res)

    def isdir(self, file_path: str):
        '''Does nothing

        .. note:: AWS S3 does not have concept of directory tree; what
           this function (and ``mkdir()`` and ``makedirs()`` should do
           and return? To be strict, it would be straightforward to
           raise ``io.UnsupportedOperation`` exception. But it just
           breaks users' applications that except quasi-compatible
           behaviour. Thus, imitating other file systems, like
           returning boolean or ``None`` would be nicer.

        '''
        # raise io.UnsupportedOperation("S3 doesn't have directory")
        pass

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        '''Does nothing

        .. note:: see discussion in ``isdir()``.
        '''
        # raise io.UnsupportedOperation("S3 doesn't have directory")
        pass

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        '''Does nothing

        .. note:: see discussion in ``isdir()``.
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
            res = self.client.head_object(Bucket=self.bucket,
                                          Key=key)
            return not res.get('DeleteMarker')
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    def rename(self, src, dst):
        '''Copies & removes the object

        Source and destination must be in the same bucket for
        ``pfio``, although AWS S3 supports inter-bucket copying.

        '''
        self._checkfork()
        source = {'Bucket': self.bucket, 'Key': os.path.join(self.cwd, src)}
        dst = os.path.join(self.cwd, dst)
        res = self.client.copy_object(Bucket=self.bucket,
                                      CopySource=source,
                                      Key=dst)
        if not res.get('CopyObjectResult'):
            # copy failed
            return
        return self.remove(source.get('Key'))

    def remove(self, file_path: str, recursive=False):
        '''Removes an object

        '''
        if recursive:
            raise io.UnsupportedOperation("Recursive delete not supported")

        self._checkfork()
        key = os.path.join(self.cwd, file_path)
        return self.client.delete_object(Bucket=self.bucket,
                                         Key=key)
