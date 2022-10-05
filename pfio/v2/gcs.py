from google.cloud import storage
from google.cloud.storage.fileio import BlobReader, BlobWriter
from google.oauth2 import service_account

from .fs import FS, FileStat

class ObjectStat(FileStat):
    def __init__(self, blob):
        self.blob = blob


class GoogleCloudStorage(FS):
    def __init__(self, bucket, prefix=None, key_path=None):
        with open(key_path) as kp:
            service_account_info = json.load(kp)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            self.client = storage.Client(credentials=credentials,
                                         project=credentials.project_id)
            self.bucket = self.client.get_bucket(bucket)
            self.bucket_name = bucket
            self.prefix = prefix
            
    def open(self, path, mode='r', **kwargs):
        blob = self.bucket.blob(os.path.join(self.prefix, path))

        if 'r' in mode:
            return BlobReader(blob, chunk_size=1024*1024, text_mode=(not 'b' in mode))

        elif 'w' in mode:
            return BlobReader(blob, chunk_size=1024*1024, text_mode=(not 'b' in mode))

        raise RuntimeError("Invalid mode")

    def list(self, prefix, recursive=False, detail=False):
        # TODO: recursive
        for blob in self.client.list_blobs(self.bucket_name, prefix=self.prefix):
            if detail:
                yield ObjectStat(blob)
            else:
                yield blob.path

    def stat(self, path):
        return ObjectStat(self.bucket.blob(path))

    def isdir(self, path):
        return False

    def mkdir(self, path):
        pass

    def makedirs(self, path):
        pass

    def exists(self, path):
        return self.bucket.blob(path).exists()

    def rename(self, src, dst):
        source_blob = self.bucket.blob(src)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        # Returns Blob destination
        self.bucket.copy_blob(source_blob, self.bucket, dst)
        self.bucket.delete_blob(blob_name)

    def remove(self, path, recursive=False):
        self.bucket.delete_blob(path)
