'''
A cache system for remote file system that stores cache as mmap'ed file locally

Of course, it's read only
'''

import io
import os
import shutil
import tempfile
from dataclasses import dataclass
from types import TracebackType
from typing import Optional


@dataclass(frozen=True)
class _Range:
    start: int
    length: int
    cached: bool = False

    def overlap(self, rhs) -> bool:
        return (self.start - rhs.right) * (self.right - rhs.start) < 0

    @property
    def right(self):
        return self.start + self.length

    def includes(self, rhs) -> bool:
        return (self.start <= rhs.start) and (rhs.right <= self.right)

    def merge(self, rhs):
        assert self.overlap(rhs)
        assert self.cached == rhs.cached
        return _Range(min(self.start, rhs.start), max(self.right, rhs.right),
                      cached=self.cached)


class _CachedWrapper:
    '''A transparent local cache for remote files

    TODO: add document here
    '''

    def __init__(self, fileobj, size, cachedir=None, close_on_close=False):
        self.fileobj = fileobj
        self.cachedir = cachedir
        self.size = size
        assert size > 0
        if cachedir is None:
            basedir = os.getenv('XDG_CACHE_HOME')
            if basedir is None:
                basedir = os.path.join(os.getenv('HOME'), ".cache")
            self.cachedir = os.path.join(basedir, "pfio")
        os.makedirs(self.cachedir, exist_ok=True)
        self.cachefp = tempfile.NamedTemporaryFile(delete=True,
                                                   dir=self.cachedir)
        # self.cachefp = open('cache.file', 'rwb')
        # self.cachefp = os.open('cache.file', os.O_RDWR|os.O_TRUNC)
        # TODO: make this tree if the size gets too long for O(n) scan
        self.ranges = [_Range(0, size)]
        self._closed = False
        self._frozen = False
        self.close_on_close = close_on_close

    def close(self):
        if not self._closed:
            self._closed = True
            self.cachefp.close()
            if self.close_on_close:
                self.fileobj.close()

    def preserve(self, dest):
        # Hard link and save them
        try:
            os.link(self.cachefp.name, dest)
            self.cachefp.close()
        except OSError:
            # TODO: check errno to make sure handling the
            # 'different-drive' error.
            shutil.copyfile(self.cachefp.name, dest)

        self.cachefp = open(dest, 'rb')
        self._frozen = True

    def _get_range(self, r) -> (bytearray, _Range):
        # print('get range:', r)
        if r.cached:
            return os.pread(self.cachefp.fileno(), r.length, r.start), r

        assert not self._frozen
        self.fileobj.seek(r.start, io.SEEK_SET)
        data = self.fileobj.read(r.length)
        written = os.pwrite(self.cachefp.fileno(), data, r.start)
        if written < 0:
            raise RuntimeError("bad file descriptor")
        # print(written, "/", r.length, "bytes written at", r.start)
        return data, _Range(r.start, r.length, True)

    def _read(self, size):
        new_ranges = []
        streak = []
        for data, r in self._read2(size):
            if r.length > 0:
                if r.cached:
                    streak.append(r)
                else:
                    if streak:
                        start = streak[0].start
                        length = sum(s.length for s in streak)
                        new_ranges.append(_Range(start, length, cached=True))
                        streak = []
                    new_ranges.append(r)

            if data is not None:
                yield data

        if streak:
            start = streak[0].start
            length = sum(s.length for s in streak)
            new_ranges.append(_Range(start, length, cached=True))

        self.ranges = new_ranges

    def _read2(self, size):

        r0 = _Range(self.pos, size)
        # print("read =>", r0)
        for r1 in self.ranges:
            if not r1.overlap(r0):
                yield None, r1
                continue

            # [r0 [ ... r1] never happens as the first r1 always starts with 0
            assert r1.start <= r0.start

            # [r1 [ r0 ] ] r1 cached; prevent unnecessary area split
            if r1.includes(r0) and r1.cached:
                data = os.pread(self.cachefp.fileno(), r0.length, r0.start)
                # print(r1, 'includes', r0, 'len(r0)?=', len(data))
                yield data, r1
                continue

            yield None, _Range(r1.start, r0.start - r1.start, r1.cached)

            # [ r1 [ r0 ] ]
            if r0.right < r1.right:
                yield self._get_range(_Range(r0.start, r0.length, r1.cached))
                # print('[ r1 [ r0 ] ]', r, len(data))
                yield None, _Range(r0.right, r1.right - r0.right, r1.cached)
                continue

            # [ r1 [ ] r0 ]
            yield self._get_range(_Range(r0.start, r1.right - r0.start,
                                         r1.cached))
            # print('[ r1 [ ] r0 ]', r, len(data))

            r0 = _Range(r1.right, r0.right - r1.right)

    def _read_all_cache(self):
        for r in self.ranges:
            if r.cached:
                data = os.pread(self.cachefp.fileno(), r.length, r.start)
                # print(r1, 'includes', r0, 'len(r0)?=', len(data))
                yield data, r
            else:
                yield None, r

    def read(self, size=-1) -> bytes:
        if self._closed:
            raise RuntimeError("closed")

        if size < 0:
            size = self.size - self.pos

        buf = bytearray(size)
        offset = 0
        # TODO: unnecessary copy; optimize with os.readv?
        for data in self._read(size):
            buf[offset:offset+len(data)] = data
            offset += len(data)

        self.pos += len(buf)
        self.pos %= self.size
        if self.pos != self.fileobj.tell():
            self.fileobj.seek(self.pos, io.SEEK_SET)
        return bytes(buf)

    def readline(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type: Optional[BaseException],
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

    def tell(self):
        return self.pos

    def truncate(self, size=None):
        raise io.UnsupportedOperation('truncate')

    def seek(self, pos, whence=io.SEEK_SET):
        # print(dir(self.fileobj))
        if whence in [0, io.SEEK_SET]:
            if pos < 0:
                raise OSError(22, "[Errno 22] Invalid argument")
        elif whence in [1, io.SEEK_CUR]:
            pos += self.pos
        elif whence in [2, io.SEEK_END]:
            pos += self.size
        else:
            raise ValueError('Wrong whence value: {}'.format(whence))

        if pos < 0:
            raise OSError(22, "[Errno 22] Invalid argument")
        self.pos = pos
        self.fileobj.seek(self.pos, io.SEEK_SET)
        return self.pos

    def writable(self):
        return False

    def write(self, data):
        raise io.UnsupportedOperation('not writable')

    def readall(self):
        return self.read(-1)

    def readinto(self, b):
        buf = self.read(len(b))
        b[:len(buf)] = buf
        return len(buf)


class CachedWrapper(_CachedWrapper):
    '''A transparent local cache for remote files

    TODO: add document here
    '''

    def __init__(self, fileobj, size, cachedir=None, close_on_close=False, pagesize=16*1024*1024):
        super().__init__(fileobj, size, cachedir, close_on_close)
        assert pagesize > 0
        self.pagesize = pagesize
        pagecount = size // pagesize
        self.ranges = [_Range(i * self.pagesize, self.pagesize, cached=False)
                       for i in range(pagecount)]

        remain = size % self.pagesize
        if remain > 0:
            self.ranges.append(_Range(pagecount*self.pagesize, remain, cached=False))

    def read(self, size=-1) -> bytes:
        if self._closed:
            raise RuntimeError("closed")

        if size < 0:
            size = self.size - self.pos

        start = self.pos // self.pagesize
        end = (self.pos + size) // self.pagesize
        for i in range(start, end + 1):
            r = self.ranges[i]
            
            if not r.cached:
                assert not self._frozen
                self.fileobj.seek(r.start, io.SEEK_SET)
                data = self.fileobj.read(r.length)
                written = os.pwrite(self.cachefp.fileno(), data, r.start)
                if written < 0:
                    raise RuntimeError("bad file descriptor")

                self.ranges[i] = _Range(r.start, r.length, cached=True)
                # print(written, "/", r.length, "bytes written at", r.start)
                
        buf = os.pread(self.cachefp.fileno(), size, self.pos)

        self.pos += len(buf)
        self.pos %= self.size
        if self.pos != self.fileobj.tell():
            self.fileobj.seek(self.pos, io.SEEK_SET)
        return buf
