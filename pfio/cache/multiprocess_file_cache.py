import errno
import fcntl
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
    '''Multiprocess-safe cache system with local filesystem

    Stores cache data in local temporary files, created in
    ``~/.pfio/cache`` by default. Cache data is
    automatically deleted after the object is collected. When this
    object is not correctly closed, (e.g., the process killed by
    SIGTERM), the cache remains after the death of process.

    This class supports handling a cache from multiple processes.
    A MultiprocessFileCache object can be handed over to another process
    through the pickle. Calling ``get`` and ``put`` in each process will
    look into the same cache files which are created by the initializer.
    The temporary cache files will persist until the MultiprocessFileCache
    object is destroyed in the original process it is created.
    This means that even after the worker processes are destroyed,
    the MultiprocessFileCache object can be passed to another processes,
    with the cache files remain accessible.

    Arguments:
        length (int): Length of the cache array.

        do_pickle (bool):
            Do automatic pickle and unpickle inside the cache.

        dir (str): The path to the directory to place cache data in
            case home directory is not backed by fast storage device.

    '''

    def __init__(self, length, do_pickle=False,
                 dir=None, verbose=False):
        self.length = length
        self.do_pickle = do_pickle
        self.verbose = verbose
        assert self.length > 0

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

    def _get(self, i):
        assert 0 <= i < self.length

        offset = self.buflen * i
        index_fd = os.open(self.index_file.name, os.O_RDONLY | os.O_NOATIME)
        fcntl.flock(index_fd, fcntl.LOCK_SH)
        index_entry = os.pread(index_fd, self.buflen, offset)
        (o, l) = unpack('Qq', index_entry)
        if l < 0 or o < 0:
            fcntl.flock(index_fd, fcntl.LOCK_UN)
            os.close(index_fd)
            return None

        data_fd = os.open(self.data_file.name, os.O_RDONLY | os.O_NOATIME)
        data = os.pread(data_fd, l, o)
        assert len(data) == l

        os.close(data_fd)
        fcntl.flock(index_fd, fcntl.LOCK_UN)
        os.close(index_fd)

        return data

    def put(self, i, data):
        if self._frozen:
            return

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
        if self.closed:
            return
        assert 0 <= i < self.length

        index_offset = self.buflen * i
        index_fd = os.open(self.index_file.name, os.O_RDWR)
        fcntl.flock(index_fd, fcntl.LOCK_EX)
        buf = os.pread(index_fd, self.buflen, index_offset)
        (o, l) = unpack('Qq', buf)

        if l >= 0 and o >= 0:
            # Already data exists
            fcntl.flock(index_fd, fcntl.LOCK_UN)
            os.close(index_fd)
            return False

        data_fd = os.open(self.data_file.name, os.O_APPEND | os.O_WRONLY)
        data_pos = os.lseek(data_fd, 0, os.SEEK_END)
        index_entry = pack('Qq', data_pos, len(data))
        assert os.pwrite(index_fd, index_entry, index_offset) == self.buflen
        assert os.pwrite(data_fd, data, data_pos) == len(data)

        os.close(data_fd)
        fcntl.flock(index_fd, fcntl.LOCK_UN)
        os.close(index_fd)

        return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        if not self.closed and os.getpid() == self._master_pid:
            self.data_file.close()
            self.index_file.close()
            self.closed = True
            self.data_file = None
            self.index_file = None

    def preload(self, name):
        '''Load the cache saved by ``preserve()``

        After loading the files, no data can be added to the cache.
        ``name`` is the prefix of the persistent files.

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

        .. note:: This feature is experimental.

        '''

        index_file = os.path.join(self.dir, '{}.cachei'.format(name))
        data_file = os.path.join(self.dir, '{}.cached'.format(name))

        if any(os.path.exists(p) for p in (index_file, data_file)):
            raise ValueError('Specified cache name "{}" already exists in {}'
                             .format(name, self.dir))

        fd = os.open(self.index_file.name, os.O_RDONLY)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.link(self.index_file.name, index_file)
            os.link(self.data_file.name, data_file)

        except OSError as ose:
            # Lock acquisition error -> No problem, since other worker
            # should be already working on it
            if ose.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
