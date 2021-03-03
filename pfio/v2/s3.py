import io
import os
from types import TracebackType
from typing import Optional, Type

import boto3

from .fs import FS


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
    def __init__(self, bucket=None, endpoint=None):
        self.bucket = bucket
        self.endpoint = endpoint

        # boto3.set_stream_logger()

        # import botocore
        # botocore.session.Session().set_debug_logger()

        # TODO: update from real env 'AWS_ACCESS_KEY_ID' in os.getenv():
        kwargs = {
            'aws_access_key_id': 'me@EXAMPLE.COM',
            'aws_secret_access_key': 'XXXXXXX',
        }
        if endpoint is not None:
            kwargs['endpoint_url'] = endpoint

        self.client = boto3.client('s3', **kwargs)

    def open(self, path, mode='r', **kwargs):
        if 'a' in mode:
            io.UnsupportedOperation('Append is not supported')

        if 'r' in mode:
            return _ObjectReader(self.client, self.bucket, path, mode, kwargs)

        elif 'w' in mode:
            return _ObjectWriter(self.client, self.bucket, path, mode, kwargs)

        else:
            raise RuntimeError(f'Unknown option: {mode}')

    def list(self, prefix: str = None, recursive=False):
        # TODO: recursive list
        res = self.client.list_objects_v2(Bucket=self.bucket)
        print(res['Name'])
        # kc = res['KeyCount']
        if 'Contents' in res:
            for k in res['Contents']:
                yield k

    def stat(self, path):
        raise NotImplementedError()
        # return FileStat(os.stat(path), path)

    def isdir(self, file_path: str):
        raise os.UnsupportedOperation("S3 doesn't have directory")

    def mkdir(self, file_path: str, mode=0o777, *args, dir_fd=None):
        raise os.UnsupportedOperation("S3 doesn't have directory")

    def makedirs(self, file_path: str, mode=0o777, exist_ok=False):
        raise os.UnsupportedOperation("S3 doesn't have directory")

    def exists(self, file_path: str):
        raise NotImplementedError()

    def rename(self, src, dst):
        raise os.UnsupportedOperation("S3 doesn't support rename")

    def remove(self, file_path: str, recursive=False):
        if recursive:
            raise os.UnsupportedOperation("Recursive delete not supported")

        return self.client.delete_object(Bucket=self.bucket,
                                         Key=file_path)
