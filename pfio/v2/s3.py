import os
import shutil

import boto3

from .fs import FS, open_wrapper


class S3(FS):
    def __init__(self, bucket=None, endpoint=None):
        self.bucket = bucket
        self.endpoint = endpoint

        # boto3.set_stream_logger()

        # import botocore
        # botocore.session.Session().set_debug_logger()

        # if 'AWS_ACCESS_KEY_ID' in os.getenv():
        # aws_access_key_id='me@EXAMPLE.COM',
        # aws_secret_access_key='XXXXXXX',
        kwargs = {}
        if endpoint is not None:
            kwargs['endpoint_url'] = endpoint

        self.client = boto3.client('s3', *kwargs)

    @open_wrapper
    def open(self, file_path, mode='r',
             buffering=-1, encoding=None, errors=None,
             newline=None, closefd=True, opener=None):
        raise NotImplementedError()

    def subfs(self, rel_path):
        raise NotImplementedError()

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
            return shutil.rmtree(file_path)
        if os.path.isdir(file_path):
            return os.rmdir(file_path)

        return os.remove(file_path)
