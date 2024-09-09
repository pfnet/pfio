import base64
import json
import io
import os
from types import TracebackType
from typing import Optional, Type

from google.cloud import storage
from google.cloud.storage.fileio import BlobReader, BlobWriter
from google.oauth2 import service_account

from .fs import FS, FileStat

def _normalize_key(key: str) -> str:
    key = os.path.normpath(key)
    if key.startswith("/"):
        return key[1:]
    else:
        return key

class ObjectStat(FileStat):
    def __init__(self, blob):
        self.path = blob.name
        self.size = blob.size
        self.metadata = blob.metadata
        self.crc32c = blob.crc32c
        self.md5_hash = base64.b64decode(blob.md5_hash).hex()
        self.filename = os.path.basename(blob.name)

    def isdir(self):
        return self.size == 0 and self.path.endswith('/')
    

class _ObjectTextWriter:
    def __init__(self, blob, chunk_size):
        # self.client = client
        # self.bucket = bucket
        # self.key = key
        # self.mode = mode
        self._init_buf()
        self.blob = blob
        self.mpu_chunksize = chunk_size
        # self.mpu_id = None
        # self.parts = []

    def _init_buf(self):
        self.buf = io.StringIO()

    def flush(self):
        # A part must be more than 8 MiB in S3
        if len(self.buf.getvalue()) < 8 * 1024 * 1024:
            return
        self._flush()

    # TODO: GCS対応
    def _flush(self):
        # Send buffer as a part
        # c = self.client
        # b = self.bucket
        # k = self.key

        data = self.buf.getvalue()
        # md5 = base64.b64encode(
        #     hashlib.md5(data.encode()).digest()
        # ).decode()
        # num = len(self.parts) + 1
        self.blob.upload_from_string(data)

        # res = c.upload_part(Body=data, Bucket=b, Key=k,
        #                     PartNumber=num,
        #                     UploadId=self.mpu_id,
        #                     ContentLength=len(data),
        #                     ContentMD5=md5)
        # self.parts.append({'ETag': res['ETag'], 'PartNumber': num})

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
        self.blob.upload_from_string(self.buf.getvalue())
        self.buf = None

    def __enter__(self):
        return self

    # TODO: 対応
    def __exit__(self, exc_type: Optional[Type[BaseException]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]):
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


class GoogleCloudStorage(FS):
    '''Google Cloud Storage wrapper

    ``key_path`` argument is a path to credential files of
    IAM service account. The path to the file can be set to
    the environmental variable``GOOGLE_APPLICATION_CREDENTIALS``
    instead.

    .. note:: This is an experimental implmentation.

    '''

    def __init__(self, bucket: str, prefix=None, key_path=None):
        self.bucket_name = bucket
        self.key_path = key_path

        if prefix is not None:
            self.cwd = prefix
        else:
            self.cwd = ''

        self._reset()

    def _reset(self):
        if self.key_path is None:
            self.client = storage.Client()
        else:
            with open(self.key_path) as kp:
                service_account_info = json.load(kp)
                credentials = service_account.Credentials.\
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
        self.bucket = self.client.get_bucket(self.bucket_name)
        assert self.bucket
        self.bucket_name = self.bucket_name

    def open(self, path, mode='r', **kwargs):
        blob = self.bucket.blob(os.path.join(self.cwd, path))

        if 'r' in mode:
            return BlobReader(blob, chunk_size=1024*1024)

        elif 'w' in mode:
            if 'b' in mode:
                return BlobWriter(blob, chunk_size=1024*1024)
            else:
                return _ObjectTextWriter(blob, chunk_size=1024*1024)

        raise RuntimeError("Invalid mode")

    def list(self, prefix: Optional[str] = "", recursive=False, detail=False):
        #  TODO: recursive
        # assert recursive, "gcs.list recursive=False no supported yet"

        path = os.path.join(self.cwd, "" if prefix is None else prefix)
        if path:
            path = os.path.normpath(path)

        for blob in self.bucket.list_blobs():
            # prefix=path):
            if detail:
                yield ObjectStat(blob)
            else:
                yield blob.name

    def stat(self, path):
        return ObjectStat(self.bucket.get_blob(path))

    def isdir(self, path):
        return False

    def mkdir(self, path):
        pass

    def makedirs(self, path):
        pass

    def exists(self, path):
        return self.bucket.blob(path).exists()

    def rename(self, src, dst):
        # source_blob = self.bucket.blob(src)
        # dest = self.client.bucket(dst)

        # Returns Blob destination
        # self.bucket.copy_blob(source_blob, self.bucket, dst)
        # self.bucket.delete_blob(src)
        pass

    def remove(self, path, recursive=False):
        self.bucket.delete_blob(path)

    def _canonical_name(self, file_path: str) -> str:
        path = os.path.join(self.cwd, file_path)
        norm_path = _normalize_key(path)

        return f"gs://{self.hostname}/{self.bucket}/{norm_path}"