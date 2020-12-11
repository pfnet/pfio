import os
from struct import pack, unpack, calcsize
import threading
import tempfile
import warnings

from pfio import cache
import pickle

_DEFAULT_CACHE_PATH = os.path.join(
    os.getenv('HOME'), ".pfio", "cache")


class LockContext:
    def __init__(self, locked_lock):
        self.locked_lock = locked_lock

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.locked_lock.unlock()


class RWLock:
    '''Reader-writer lock

    TODO(kuenishi): Add unit tests

    '''

    def __init__(self):
        self.cv = threading.Condition()
        self.writer = None
        self.reader = set()

    def rdlock(self):
        with self.cv:
            self.cv.wait_for(lambda: self.writer is None)
            self.reader.add(threading.get_ident())
            return LockContext(self)

    def wrlock(self):
        with self.cv:
            thread_id = threading.get_ident()
            self.cv.wait_for(lambda: self.writer is None and
                             self.writer != thread_id and
                             len(self.reader) == 0)
            self.writer = thread_id
            return LockContext(self)

    def unlock(self):
        with self.cv:
            thread_id = threading.get_ident()
            if self.writer == thread_id:
                self.writer = None
            else:
                self.reader.remove(thread_id)
            self.cv.notify_all()


class DummyLock:
    '''Dummy class for multithread-unsafe fast cache class
    '''

    def __init__(self):
        pass

    def rdlock(self):
        return LockContext(self)

    def wrlock(self):
        return LockContext(self)

    def unlock(self):
        pass


class FileCache(cache.Cache):
    '''Cache system with local filesystem

    Stores cache data in local temporary files, created in
    ``~/.pfio/cache`` by default. Cache data is
    automatically deleted after the object is collected. When this
    object is not correctly closed, (e.g., the process killed by
    SIGTERM), the cache remains after the death of process.

    Arguments:
        length (int): Length of the cache array.

        multithread_safe (bool): Defines multithread safety. If this
            is ``True``, reader-writer locking system based on
            ``threading.Lock`` is introduced behind the cache
            management. Major use case is with Chainer's
            ``MultithreadIterator``.

        do_pickle (bool):
            Do automatic pickle and unpickle inside the cache.

        dir (str): The path to the directory to place cache data in
            case home directory is not backed by fast storage device.

        verbose (bool):
            Print detailed logs of the cache.
    '''

    def __init__(self, length, multithread_safe=False, do_pickle=False,
                 dir=None, verbose=False):
        self._multithread_safe = multithread_safe
        self.length = length
        self.do_pickle = do_pickle
        assert self.length > 0

        if self.multithread_safe:
            self.lock = RWLock()
        else:
            self.lock = DummyLock()

        self.pos = 0
        if dir is None:
            self.dir = _DEFAULT_CACHE_PATH
        else:
            self.dir = dir
        os.makedirs(self.dir, exist_ok=True)

        self.closed = False
        self.indexfp = tempfile.NamedTemporaryFile(delete=True, dir=self.dir)
        self.datafp = tempfile.NamedTemporaryFile(delete=True, dir=self.dir)

        # allocate space to store 2n 64bit unsigned integers
        # 16 bytes * n chunks
        # Size must be smaller than max value of signed long long
        buf = pack('Qq', 0, -1)
        self.buflen = calcsize('Qq')
        assert self.buflen == 16
        for i in range(self.length):
            offset = self.buflen * i
            r = os.pwrite(self.indexfp.fileno(), buf, offset)
            assert r == self.buflen
        self.verbose = verbose
        if self.verbose:
            print('created index file:', self.indexfp.name)
            print('created data file:', self.datafp.name)

        self._frozen = False

    def __len__(self):
        return self.length

    @property
    def frozen(self):
        return self._frozen

    @property
    def multiprocess_safe(self):
        # If it's preseved/preloaded, then the file contents are
        # fixed.
        return self._frozen

    @property
    def multithread_safe(self):
        return self._multithread_safe

    def get(self, i):
        if self.closed:
            return
        data = self._get(i)
        if self.do_pickle and data:
            data = pickle.loads(data)
        return data

    def _get(self, i):
        assert i >= 0 and i < self.length
        offset = self.buflen * i
        with self.lock.rdlock():
            buf = os.pread(self.indexfp.fileno(), self.buflen, offset)
            (o, l) = unpack('Qq', buf)
            if l < 0 or o < 0:
                return None

            data = os.pread(self.datafp.fileno(), l, o)
            assert len(data) == l
            return data

    def put(self, i, data):
        assert not self._frozen
        try:
            if self.do_pickle:
                data = pickle.dumps(data)
            return self._put(i, data)

        except OSError as ose:
            # Disk full (ENOSPC) possibly by cache; just warn and keep running
            if ose.errno == 28:
                warnings.warn(ose.strerror, RuntimeWarning)
                return False
            else:
                raise ose

    def _put(self, i, data):
        if self.closed:
            return
        assert i >= 0 and i < self.length
        offset = self.buflen * i

        with self.lock.wrlock():
            buf = os.pread(self.indexfp.fileno(), self.buflen, offset)
            (o, l) = unpack('Qq', buf)
            if l >= 0 and o >= 0:
                # Already data exists
                return False

            pos = self.pos

            '''Notes on possibility of partial write

            write(3) says partial writes ret<nbyte may happen in
            case nbytes>PIPE_BUF. In Linux 5.0 PIPE_BUF is
            4096 so partial writes do not happen when writing
            index, but they may happen when writing data. We
            hope it is rare, it seems to happen mostly in case
            of multiple writer processes, disk full and
            ``EINTR``.

            CPython does care this case by retrying
            ``pwrite(2)`` as long as it returns ``-1`` . But
            returns when the return value is positive. We'd
            better care that case.

            '''
            buf = pack('Qq', pos, len(data))
            r = os.pwrite(self.indexfp.fileno(), buf, offset)
            assert r == self.buflen

            current_pos = pos
            while current_pos - pos < len(data):
                r = os.pwrite(self.datafp.fileno(),
                              data[current_pos-pos:], current_pos)
                assert r > 0
                current_pos += r
            assert current_pos - pos == len(data)

            self.pos += len(data)
            return True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def close(self):
        with self.lock.wrlock():
            if not self.closed:
                self.closed = True
                self.indexfp.close()
                self.datafp.close()
                self.indexfp = None
                self.datafp = None

    def preload(self, name):
        '''Load the cache saved by ``preserve()``

        After loading the files, no data can be added to the cache.
        ``name`` is the prefix of the persistent files. To use cache
        in ``multiprocessing`` environment, call this method at every
        forked process, except the process that called ``preserve()``.

        Returns:
            bool: Returns True if succeed.

        .. note:: This feature is experimental.

        '''
        if self._frozen:
            if self.verbose:
                print("Failed to preload the cache from {}: "
                      "The cache is already frozen."
                      .format(name))
            return False

        indexfile = os.path.join(self.dir, '{}.cachei'.format(name))
        datafile = os.path.join(self.dir, '{}.cached'.format(name))

        if any(not os.path.exists(p) for p in (indexfile, datafile)):
            if self.verbose:
                print('Failed to ploread the cache from {}: '
                      'The specified cache not found in {}'
                      .format(name, self.dir))
            return False

        with self.lock.wrlock():
            # Hard link and save them
            self.indexfp.close()
            self.datafp.close()

            self.indexfp = open(indexfile, 'rb')
            self.datafp = open(datafile, 'rb')
            self._frozen = True
        return True

    def preserve(self, name):
        '''Preserve the cache as persistent files on the disk

        Once the cache is preserved, cache files will not be removed
        at cache close. To read data from preserved files, use
        ``preload()`` method. After preservation, no data can be added
        to the cache.  ``name`` is the prefix of the persistent
        files.

        The preserved cache can also be preloaded by
        :class:`~MultiprocessFileCache`.

        Returns:
            bool: Returns True if succeed.

        .. note:: This feature is experimental.

        '''

        indexfile = os.path.join(self.dir, '{}.cachei'.format(name))
        datafile = os.path.join(self.dir, '{}.cached'.format(name))

        if any(os.path.exists(p) for p in (indexfile, datafile)):
            if self.verbose:
                print('Specified cache named "{}" already exists in {}'
                      .format(name, self.dir))
            return False

        with self.lock.wrlock():
            # Hard link and save them
            os.link(self.indexfp.name, indexfile)
            os.link(self.datafp.name, datafile)
            self.indexfp.close()
            self.datafp.close()

            self.indexfp = open(indexfile, 'rb')
            self.datafp = open(datafile, 'rb')
            self._frozen = True
        return True
