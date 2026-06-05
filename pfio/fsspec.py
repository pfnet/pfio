'''fsspec integration for PFIO.

This module exposes :mod:`pfio.v2` backends (Local / S3 / HDFS) through the
`fsspec <https://filesystem-spec.readthedocs.io/>`_ ``AbstractFileSystem``
interface so that libraries built on top of fsspec (pandas, pyarrow, dask,
zarr, ...) can use PFIO as their storage backend.

Each PFIO backend is registered under its own protocol so that it does not
clobber fsspec's built-in implementations (e.g. ``s3`` provided by s3fs):

* ``pfio-file://`` -> :class:`pfio.v2.Local`
* ``pfio-s3://``   -> :class:`pfio.v2.S3`
* ``pfio-hdfs://`` -> :class:`pfio.v2.Hdfs`

These protocols are registered automatically through the ``fsspec.specs``
entry points declared in ``pyproject.toml``.  To make PFIO handle a standard
protocol such as ``s3://`` instead, call :func:`register`.

``fsspec`` is an optional dependency.  Importing :mod:`pfio` itself never
requires fsspec; this module is only imported lazily by fsspec's entry point
machinery (or explicitly by the user), at which point fsspec is guaranteed to
be installed.
'''
import threading
from datetime import datetime
from typing import Optional

try:
    from fsspec.spec import AbstractFileSystem
    _HAS_FSSPEC = True
except ImportError:
    _HAS_FSSPEC = False


if _HAS_FSSPEC:
    class _PfioFileSystem(AbstractFileSystem):
        '''Common base wrapping a :class:`pfio.v2.FS` behind fsspec.

        Subclasses bind a single PFIO backend via :attr:`_pfio_scheme` and
        implement :meth:`_get_fs_and_path`, which resolves an fsspec path to a
        concrete PFIO ``FS`` instance and the path relative to it.

        The wrapped PFIO ``FS`` instances are created lazily and reused.  PFIO
        backends detect ``fork()`` and reconnect at each operation boundary, so
        sharing a single instance (as fsspec's instance cache does) is safe.
        The PFIO instances are intentionally *not* part of the pickled state;
        ``AbstractFileSystem.__reduce__`` reconstructs the filesystem from its
        ``storage_options``, after which they are lazily recreated.
        '''

        _pfio_scheme: Optional[str] = None

        def __init__(self, **storage_options):
            super().__init__(**storage_options)
            self._fs = None
            self._fs_lock = threading.Lock()

        def _get_fs_and_path(self, path):
            '''Return ``(pfio_fs, path_relative_to_fs)`` for ``path``.'''
            raise NotImplementedError

        @staticmethod
        def _stat_to_info(name, st):
            '''Convert a PFIO ``FileStat`` into an fsspec info dict.'''
            if st.isdir():
                # S3PrefixStat reports size == -1; normalize directories to 0.
                return {"name": name, "size": 0, "type": "directory"}
            info = {
                "name": name,
                "size": st.size if st.size is not None else 0,
                "type": "file",
            }
            mtime = getattr(st, "last_modified", None)
            if mtime:
                info["mtime"] = mtime
            return info

        def _open(self, path, mode="rb", block_size=None, autocommit=True,
                  cache_options=None, **kwargs):
            fs, rel = self._get_fs_and_path(path)
            return fs.open(rel, mode)

        def ls(self, path, detail=True, **kwargs):
            fs, rel = self._get_fs_and_path(path)
            # fsspec contract: ls() of a file returns a single-element list
            # with that file's info, not a directory listing.
            if fs.exists(rel) and not fs.isdir(rel):
                info = self.info(path)
                return [info] if detail else [info["name"]]
            base = self._strip_protocol(path).rstrip("/")
            out = []
            for st in fs.list(rel, recursive=False, detail=True):
                # PFIO returns names relative to the listed path (basename for
                # Local, key-relative for S3); rejoin to a full fsspec path.
                name = base + "/" + st.filename.rstrip("/") if base \
                    else st.filename.rstrip("/")
                out.append(self._stat_to_info(name, st))
            if detail:
                return out
            return sorted(e["name"] for e in out)

        def info(self, path, **kwargs):
            fs, rel = self._get_fs_and_path(path)
            name = self._strip_protocol(path)
            st = fs.stat(rel)
            return self._stat_to_info(name, st)

        def exists(self, path, **kwargs):
            fs, rel = self._get_fs_and_path(path)
            return fs.exists(rel)

        def isdir(self, path):
            fs, rel = self._get_fs_and_path(path)
            return fs.isdir(rel)

        def isfile(self, path):
            fs, rel = self._get_fs_and_path(path)
            return fs.exists(rel) and not fs.isdir(rel)

        def mkdir(self, path, create_parents=True, **kwargs):
            fs, rel = self._get_fs_and_path(path)
            if create_parents:
                fs.makedirs(rel, exist_ok=True)
            else:
                fs.mkdir(rel)

        def makedirs(self, path, exist_ok=False):
            fs, rel = self._get_fs_and_path(path)
            fs.makedirs(rel, exist_ok=exist_ok)

        def rmdir(self, path):
            fs, rel = self._get_fs_and_path(path)
            fs.remove(rel, recursive=False)

        def _rm(self, path):
            fs, rel = self._get_fs_and_path(path)
            fs.remove(rel, recursive=False)

        def modified(self, path):
            fs, rel = self._get_fs_and_path(path)
            st = fs.stat(rel)
            return datetime.fromtimestamp(st.last_modified)

        def created(self, path):
            fs, rel = self._get_fs_and_path(path)
            st = fs.stat(rel)
            created = getattr(st, "created", None)
            if created is None:
                raise NotImplementedError(
                    "{} does not expose creation time".format(
                        type(self).__name__))
            return datetime.fromtimestamp(created)

    class PfioFileFileSystem(_PfioFileSystem):
        '''fsspec filesystem backed by :class:`pfio.v2.Local`.'''

        protocol = "pfio-file"
        root_marker = "/"
        _pfio_scheme = "file"

        def _get_fs_and_path(self, path):
            rel = self._strip_protocol(path)
            with self._fs_lock:
                if self._fs is None:
                    from pfio.v2 import Local

                    # cwd="" falls back to os.getcwd(), but every operation
                    # receives an absolute path which os.path.join honors.
                    self._fs = Local(cwd="", scheme="file")
            return self._fs, rel

    class PfioS3FileSystem(_PfioFileSystem):
        '''fsspec filesystem backed by :class:`pfio.v2.S3`.

        Paths are ``pfio-s3://<bucket>/<key>``.  Because PFIO's ``S3`` takes
        the bucket as a constructor argument while fsspec uses a single
        instance for all paths, one ``S3`` instance is created (lazily) per
        bucket and reused.
        '''

        protocol = "pfio-s3"
        root_marker = ""
        _pfio_scheme = "s3"

        # Only connection-defining options; the bucket comes from the path.
        _S3_CONN_KEYS = (
            "endpoint", "create_bucket", "aws_access_key_id",
            "aws_secret_access_key", "mpu_chunksize", "buffering",
            "connect_timeout", "read_timeout",
        )

        def __init__(self, **storage_options):
            super().__init__(**storage_options)
            self._fs_cache = {}
            self._s3_kwargs = {
                k: storage_options[k]
                for k in self._S3_CONN_KEYS
                if k in storage_options
            }

        def _get_fs_and_path(self, path):
            p = self._strip_protocol(path).lstrip("/")
            bucket, _, key = p.partition("/")
            if not bucket:
                raise ValueError(
                    "S3 path must contain a bucket: {!r}".format(path))
            with self._fs_lock:
                fs = self._fs_cache.get(bucket)
                if fs is None:
                    from pfio.v2 import S3
                    fs = S3(bucket=bucket, prefix="", scheme="s3",
                            **self._s3_kwargs)
                    self._fs_cache[bucket] = fs
            return fs, key

    class PfioHdfsFileSystem(_PfioFileSystem):
        '''fsspec filesystem backed by :class:`pfio.v2.Hdfs`.'''

        protocol = "pfio-hdfs"
        root_marker = "/"
        _pfio_scheme = "hdfs"

        @classmethod
        def _strip_protocol(cls, path):
            # PFIO resolves the HDFS nameservice from hdfs-site.xml and ignores
            # any netloc, so drop it and keep only the path component.
            if isinstance(path, str) and "://" in path:
                from urllib.parse import urlparse
                path = urlparse(path).path
            return super()._strip_protocol(path)

        def _get_fs_and_path(self, path):
            rel = self._strip_protocol(path)
            with self._fs_lock:
                if self._fs is None:
                    from pfio.v2 import Hdfs
                    self._fs = Hdfs(cwd="/", scheme="hdfs")
            return self._fs, rel

    def register(*, s3=False, hdfs=False, file=False, clobber=True):
        '''Register PFIO backends under fsspec's standard protocols.

        The ``pfio-file`` / ``pfio-s3`` / ``pfio-hdfs`` protocols are always
        available via entry points.  This helper additionally binds PFIO to the
        standard ``file`` / ``s3`` / ``hdfs`` protocols on an opt-in basis,
        overriding any implementation already registered for them.

        Args:
            s3 (bool): Bind :class:`PfioS3FileSystem` to ``s3``.
            hdfs (bool): Bind :class:`PfioHdfsFileSystem` to ``hdfs``.
            file (bool): Bind :class:`PfioFileFileSystem` to ``file``.
            clobber (bool): Overwrite an existing registration.
        '''
        import fsspec
        if s3:
            fsspec.register_implementation(
                "s3", PfioS3FileSystem, clobber=clobber)
        if hdfs:
            fsspec.register_implementation(
                "hdfs", PfioHdfsFileSystem, clobber=clobber)
        if file:
            fsspec.register_implementation(
                "file", PfioFileFileSystem, clobber=clobber)
