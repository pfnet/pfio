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
        self.last_modified = head['LastModified']
        self.size = head['ContentLength']
        self.metadata = head['Metadata']
        self._head = head

    def isdir(self):
        return False


class _ObjectReader(io.BufferedReader):
    def __init__(self, client, bucket, key, mode, kwargs):
        self.client = client
        self.res = self.client.get_object(Bucket=bucket,
                                          Key=key)
        self._mode = mode
        self.body = self.res['Body']

    def read(self):
        if 'b' in self._mode:
            return self.body.read()
        else:
            return self.body.read().decode('utf-8')

    def close(self):
        return self.body.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.close()


class _ObjectWriter(io.BufferedWriter):
    def __init__(self, client, bucket, key, mode, kwargs):
        self.client = client
        self.bucket = bucket
        self.key = key
        if 'b' in mode:
            self.buf = b''
        else:
            self.buf = ''

    def write(self, buf):
        self.buf += buf

    def close(self):
        # TODO: MPU
        # See:  https://boto3.amazonaws.com/v1/documentation/
        # api/latest/reference/services/s3.html#S3.Client.put_object
        self.client.put_object(Body=self.buf,
                               Bucket=self.bucket,
                               Key=self.key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> bool:
        self.close()


class S3(FS):
    '''
    '''

    def __init__(self, bucket, prefix=None,
                 endpoint=None, create_bucket=False,
                 aws_access_key_id=None,
                 aws_secret_access_key=None):
        self.bucket = bucket
        self.endpoint = endpoint
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
        if aws_access_key_id is not None:
            kwargs['aws_access_key_id'] = aws_access_key_id
        if aws_secret_access_key is not None:
            kwargs['aws_secret_access_key'] = aws_secret_access_key
        if endpoint is not None:
            kwargs['endpoint_url'] = endpoint

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
            return _ObjectReader(self.client, self.bucket, path, mode, kwargs)

        elif 'w' in mode:
            return _ObjectWriter(self.client, self.bucket, path, mode, kwargs)

        else:
            raise RuntimeError(f'Unknown option: {mode}')

    def list(self, prefix: str = "", recursive=False):
        '''List all objects (and prefixes)

        Although there is not concept of directory in AWS S3 API,
        common prefixes shows up like directories.

        '''
        self._checkfork()
        key = os.path.join(self.cwd, prefix)

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
