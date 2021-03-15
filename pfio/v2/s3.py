import hashlib
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
        self.bucket = bucket
        self.key = key

        res = self.client.head_object(Bucket=bucket, Key=key)
        if res.get('DeleteMarker'):
            raise FileNotFoundError()

        self._mode = mode
        self.pos = 0
        self.content_length = res['ContentLength']
        self._closed = False

    def read(self, size=-1):
        s = self.pos

        if size <= 0:
            e = ''
        elif self.pos + size < self.content_length:
            e = self.pos + size

        r = 'bytes={}-{}'.format(s, e)
        print('range=', r)
        res = self.client.get_object(Bucket=self.bucket,
                                     Key=self.key,
                                     Range=r)
        body = res['Body']

        if 'b' in self._mode:
            data = body.read(size)
        else:
            data = body.read(size).decode('utf-8')

        self.pos += len(data)
        print('pos=', self.pos)
        return data

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

    def seek(self, pos, whence=io.SEEK_SET):
        if whence in [0, io.SEEK_SET]:
            self.pos = pos
        elif whence in [1, io.SEEK_CUR]:
            self.pos += pos
        elif whence in [2, io.SEEK_END]:
            self.pos += pos

        if self.content_length < self.pos:
            self.pos = self.content_length
        if self.pos < 0:
            raise OSError()
        return self.pos

    def writable(self):
        return False


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
        # print("Sent", len(data), "bytes", num)
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
            max_parts = len(self.parts)
            res = c.list_parts(Bucket=self.bucket,
                               Key=self.key,
                               UploadId=self.mpu_id, MaxParts=max_parts)

            if res['IsTruncated']:
                raise RuntimeError('truncated.')

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

    '''

    def __init__(self, bucket, prefix=None,
                 endpoint=None, create_bucket=False,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 mpu_chunksize=32*1024*1024):
        self.bucket = bucket
        if prefix is not None:
            self.cwd = prefix
        else:
            self.cwd = ''

        self.mpu_chunksize = mpu_chunksize

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
        self._checkfork()
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
