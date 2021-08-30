'''
fs.FS> interface
implementations:
open_fs(URI, container=None|zip) => Local/HDFS/S3/Zip, etc
- Local
  - subfs() -> Local
  - open_zip() -> Zip
  - open() -> FileObject
- HDFS
  - subfs() -> HDFS
  - open_zip() -> Zip
  - open() -> FileObject
- Zip
  - subfs() -> Zip
  - open_zip() -> Zip
  - open() -> FileObject
- S3 (TBD)
- GS (TBD)

For example of globally switching backend file systems::

  from pfio.v2 import local as pfio

Or::

  from pfio.v2 import Hdfs
  pfio = Hdfs()

'''
from .fs import from_url, lazify, open_url  # NOQA
from .hdfs import Hdfs, HdfsFileStat  # NOQA
from .local import Local, LocalFileStat  # NOQA
from .pathlib import Path  # NOQA
from .s3 import S3  # NOQA
from .zip import Zip, ZipFileStat  # NOQA

local = Local()
