import errno
import fcntl
import numbers
import os
import tempfile
import warnings

from struct import pack, unpack, calcsize

from pfio import cache
from pfio.cache.file_cache import _DEFAULT_CACHE_PATH
import pickle


class _NoOpenNamedTemporaryFile(object):
    """Temporary file class

    This class warps mkstemp and implements auto-clean mechanism.
    The reason why we cannot use the tempfile.NamedTemporaryFile is that
    it has an unpicklable member because it opens the created temporary file,
    which makes it impossible to pass over to worker processes.

    The auto cleanup mechanism is based on CPython tempfile implementation.
    https://github.com/python/cpython/blob/3.8/Lib/tempfile.py#L406-L446
    """

    # Set here since __del__ checks it
    name = None
    master_pid = None

    def __init__(self, dir, master_pid):
        _, self.name = tempfile.mkstemp(dir=dir)
        self.master_pid = master_pid

    def close(self, unlink=os.unlink, getpid=os.getpid):
        if self.name and self.master_pid == getpid():
            unlink(self.name)
            self.name = None

    def __del__(self):
        self.close()


class _DummyTemporaryFile(object):
    """Dummy tempfile class that imitates the _NoOpenNamedTemporaryFile

    This class is used for MultiprocessFileCache.preload.
    The cache file fed from outside shouldn't be automatically deleted
    by close(), so it uses this dummy cache class.
    """
    def __init__(self, name):
        self.name = name

    def close(self):
        pass


class MultiprocessFileCache(cache.Cache):
    '''The Multiprocess-safe cache system on a local filesystem

    Stores cache data in local temporary files, created in ``~/.pfio/cache``
    by default. It automatically deletes the cache data after the object is
    collected. When this object is not correctly closed (e.g., the process
    killed by SIGKILL), the cache remains after the process's death.

    This class supports handling a cache from multiple processes.
    A MultiprocessFileCache object can be handed over to another process
    through the pickle. Calling ``get`` and ``put`` in each process will
    look into the same cache files with flock-based locking. The temporary
    cache files will persist as long as the MultiprocessFileCache object is
    alive in the original process that creates it.
    Therefore, even after destroying the worker processes,
    the MultiprocessFileCache object can still be passed to another process.

    .. admonition:: Example

       Using MultiprocessFileCache is similar to the :class:`~NaiveCache`
       and :class:`~FileCache`.
       ::

           from pfio.cache import MultiprocessFileCache
       
           class MyDataset(torch.utils.data.Dataset):
               def __init__(self, image_paths):
                   self.paths = image_paths
                   self.cache = MultiprocessFileCache(len(image_paths), do_pickle=True)

               ...

       When iterating over the dataset, it is common to load the data
       concurrently to hide file IO bottleneck by setting higher ``num_workers``
       in PyTorch DataLoader.
       https://pytorch.org/docs/stable/data.html
       ::

           image_paths = open('/path/to/image_list.txt').read().splitlines()
           dataset = MyDataset(image_paths)
           loader = DataLoader(dataset, batch_size=64, num_workers=8)  # Parallel data loading
       
           for epoch in range(10):
               for batch in loader:
                   ...

       In this case, the dataset is distributed to each worker process
       i.e., ``__getitem__`` of the dataset will be called in a different
       process that initialized it.
       The ``MultiprocessFileCache`` object held by the dataset in each worker
       looks at the same cache file and handles the concurrent access based on
       the ``flock`` system call.
       Therefore the data inserted to the cache by a worker process
       can be accessed from another worker process safely.

       In case your task does not require concurrent data loading,
       i.e., ``num_workers=0`` in DataLoader, consider using :class:`~FileCache`
       as it has less overhead for concurrency control.

       The persisted cache files created by ``preserve()`` can be used for
       :meth:`FileCache.preload` and vice versa.

    Arguments:
        length (int): Length of the cache array.

        do_pickle (bool):
            Do automatic pickle and unpickle inside the cache.

        dir (str): The path to the directory to place cache data in
            case home directory is not backed by fast storage device.

        cache_size_limit (None or int): Limitation of the cache size in bytes.
            If the total amount of cached data reaches the limit,
            the cache will become frozen and no longer acccept further addition.
            Data already stored in the cache can be accessed normally.
            None (default) and 0 is unlimited.

        verbose (bool):
            Print detailed logs of the cache.

    '''  # NOQA

    def __init__(self, length, do_pickle=False,
                 dir=None, cache_size_limit=None, verbose=False):
        self.length = length
        self.do_pickle = do_pickle
        self.verbose = verbose
        assert self.length > 0

        if not (cache_size_limit is None or
                (isinstance(cache_size_limit, numbers.Number) and
                 0 <= cache_size_limit)):
            msg = "cache_size_limit has to be either None, zero " \
                  "(both indicate unlimited) or larger than 0. " \
                  "{} is specified.".format(cache_size_limit)
            raise ValueError(msg)
        self.cache_size_limit = cache_size_limit

        if dir is None:
            self.dir = _DEFAULT_CACHE_PATH
        else:
            self.dir = dir
        os.makedirs(self.dir, exist_ok=True)

        self.closed = False
        self._frozen = False
        self._master_pid = os.getpid()
        self.data_file = _NoOpenNamedTemporaryFile(self.dir, self._master_pid)
        self.index_file = _NoOpenNamedTemporaryFile(self.dir, self._master_pid)
        index_fd = os.open(self.index_file.name, os.O_RDWR)

        if self.verbose:
            print('created index file:', self.index_file.name)
            print('created data file:', self.data_file.name)

        try:
            fcntl.flock(index_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

            # Fill up index file by index=0, size=-1
            buf = pack('Qq', 0, -1)
            self.buflen = calcsize('Qq')
            assert self.buflen == 16
            for i in range(self.length):
                offset = self.buflen * i
                r = os.pwrite(index_fd, buf, offset)
                assert r == self.buflen
        except OSError as ose:
            # Lock acquisition error -> No problem, since other worker
            # should be already working on it
            if ose.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        finally:
            fcntl.flock(index_fd, fcntl.LOCK_UN)
            os.close(index_fd)

        # Open lazily at the first call of get or put in each child process
        self._fd_pid = None
        self.index_fd = None
        self.data_fd = None

    def __len__(self):
        return self.length

    @property
    def multiprocess_safe(self) -> bool:
        return True

    @property
    def multithread_safe(self) -> bool:
        return True

    def get(self, i):
        if self.closed:
            return
        data = self._get(i)
        if self.do_pickle and data:
            data = pickle.loads(data)
        return data

    def _open_fds(self):
        pid = os.getpid()
        if self._fd_pid != pid:
            self._fd_pid = pid
            self.index_fd = os.open(self.index_file.name, os.O_RDWR)
            self.data_fd = os.open(self.data_file.name, os.O_RDWR)

    def _get(self, i):
        assert 0 <= i < self.length

        self._open_fds()
        offset = self.buflen * i
        fcntl.flock(self.index_fd, fcntl.LOCK_SH)
        index_entry = os.pread(self.index_fd, self.buflen, offset)
        (o, l) = unpack('Qq', index_entry)
        if l < 0 or o < 0:
            fcntl.flock(self.index_fd, fcntl.LOCK_UN)
            return None

        data = os.pread(self.data_fd, l, o)
        assert len(data) == l
        fcntl.flock(self.index_fd, fcntl.LOCK_UN)
        return data

    def put(self, i, data):
        if self._frozen or self.closed:
            return False

        try:
            if self.do_pickle:
                data = pickle.dumps(data)
            return self._put(i, data)

        except OSError as ose:
            # Disk full (ENOSPC) possibly by cache; just warn and keep running
            if ose.errno == errno.ENOSPC:
                warnings.warn(ose.strerror, RuntimeWarning)
                return False
            else:
                raise ose

    def _put(self, i, data):
        assert 0 <= i < self.length
        self._open_fds()

        fcntl.flock(self.index_fd, fcntl.LOCK_EX)
        data_pos = os.lseek(self.data_fd, 0, os.SEEK_END)
        if self.cache_size_limit:
            if self.cache_size_limit < (data_pos + len(data)):
                self._frozen = True
                fcntl.flock(self.index_fd, fcntl.LOCK_UN)
                return False

        index_ofst = self.buflen * i
        buf = os.pread(self.index_fd, self.buflen, index_ofst)
        (o, l) = unpack('Qq', buf)

        if l >= 0 and o >= 0:
            # Already data exists
            fcntl.flock(self.index_fd, fcntl.LOCK_UN)
            return False

        index_entry = pack('Qq', data_pos, len(data))
        assert os.pwrite(self.index_fd, index_entry, index_ofst) == self.buflen
        assert os.pwrite(self.data_fd, data, data_pos) == len(data)
        os.fsync(self.index_fd)
        os.fsync(self.data_fd)
        fcntl.flock(self.index_fd, fcntl.LOCK_UN)
        return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        pid = os.getpid()

        if pid == self._fd_pid:
            os.close(self.data_fd)
            os.close(self.index_fd)
            self._fd_pid = None

        if not self.closed and pid == self._master_pid:
            self.data_file.close()
            self.index_file.close()
            self.closed = True
            self.data_file = None
            self.index_file = None
            self.data_fd = None
            self.index_fd = None

    def preload(self, name):
        '''Load the cache saved by ``preserve()``

        After loading the files, no data can be added to the cache.
        ``name`` is the prefix of the persistent files.

        Be noted that ``preload()`` can be called only by the master process
        i.e., the process where ``__init__()`` is called,
        in order to prevent inconsistency.

        .. note:: This feature is experimental.

        '''
        if self._frozen:
            return

        if self._master_pid != os.getpid():
            raise RuntimeError("Cannot preload a cache in a worker process")

        # Overwrite the current cache by the specified cache file.
        # This is needed to prevent the specified cache files are deleted when
        # the cache object is destroyed.
        ld_index_file = os.path.join(self.dir, '{}.cachei'.format(name))
        ld_data_file = os.path.join(self.dir, '{}.cached'.format(name))
        if any(not os.path.exists(p) for p in (ld_index_file, ld_data_file)):
            raise ValueError('Specified cache "{}" not found in {}'
                             .format(name, self.dir))

        self.data_file.close()
        self.index_file.close()
        self.data_fd = None
        self.index_fd = None
        self.data_file = _DummyTemporaryFile(ld_data_file)
        self.index_file = _DummyTemporaryFile(ld_index_file)
        self._frozen = True

    def preserve(self, name):
        '''Preserve the cache as persistent files on the disk

        Once the cache is preserved, cache files will not be removed
        at cache close. To read data from preserved files, use
        ``preload()`` method. After preservation, no data can be added
        to the cache.  ``name`` is the prefix of the persistent
        files.

        Be noted that ``preserve()`` can be called only by the master process
        i.e., the process where ``__init__()`` is called,
        in order to prevent inconsistency.

        The preserved cache can also be preloaded by :class:`~FileCache`.

        .. note:: This feature is experimental.

        '''

        if self._master_pid != os.getpid():
            raise RuntimeError("Cannot preserve a cache in a worker process")

        index_file = os.path.join(self.dir, '{}.cachei'.format(name))
        data_file = os.path.join(self.dir, '{}.cached'.format(name))

        if any(os.path.exists(p) for p in (index_file, data_file)):
            raise ValueError('Specified cache name "{}" already exists in {}'
                             .format(name, self.dir))

        self._open_fds()
        try:
            fcntl.flock(self.index_fd, fcntl.LOCK_EX)
            os.link(self.index_file.name, index_file)
            os.link(self.data_file.name, data_file)

        except OSError as ose:
            # Lock acquisition error -> No problem, since other worker
            # should be already working on it
            if ose.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        finally:
            fcntl.flock(self.index_fd, fcntl.LOCK_UN)
