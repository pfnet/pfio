import fnmatch
import os
from pathlib import PurePath

from .fs import FS
from .local import Local


class Path:
    sep = '/'

    def __init__(self, *args, fs=None, root=None):
        self._fs = fs if fs else Local()
        assert isinstance(self._fs, FS)
        self._parts = list(args)
        self._root = root

        # args starts with '/'
        if len(self._parts) > 0 and self._parts[0] == self.sep and self._root is None:
            self._parts = self._parts[1:]
            self._root = self.sep

    def __rtruediv__(self, a):
        lhs = Path(a, fs=None, root=None)
        return lhs / self

    def __truediv__(self, a):
        parts = self._parts[:]
        if isinstance(a, Path):
            parts += a._parts
        elif isinstance(a, PurePath):
            raise RuntimeError("mixed")
        else:
            parts.append(str(a))

        p = Path(*parts, fs=self._fs, root=self._root)
        return p

    @classmethod
    def cwd(cls):
        raise NotImplementedError("cwd() is unsupported on this system")

    @classmethod
    def home(cls):
        raise NotImplementedError("home() is unsupported on this system")

    def exists(self):
        return self._fs.exists(self.resolve())

    def is_absolute(self):
        return self._root is not None

    def __repr__(self):
        return "{}({} {} {})".format(self.__class__.__name__,
                                     self._root,
                                     self._parts,
                                     self._fs)

    def __str__(self):
        return os.path.join(self._root, *self._parts)

    def __fspath__(self):
        return str(self)

    def resolve(self, strict=True):
        base = self._root if self._root else self._fs.cwd
        if '..' in self._parts or '.' in self._parts:
            raise RuntimeError("TODO")
        parts = self._parts[:]
        p = Path(*parts, fs=self._fs, root=base)
        return p

    def samefile(self, other):
        return str(self.resolve()) == str(other.resolve())

    def touch(self):
        with self._fs.open(self.resolve(), 'wb') as fp:
            fp.write(b'')

    def open(self, mode='r', **kwargs):
        # TODO: handle or warn on ignoring these keyword arguments
        # buffering=-1, encoding=None, errors=None, newline=None):
        return self._fs.open(self.resolve(), mode)

    def is_dir(self):
        return self._fs.isdir(self.resolve())

    def is_file(self):
        return not self._fs.isdir(self.resolve())

    def glob(self, pattern):
        '''glob

        TODO: this implemention is not smart enough in case of large
        subdicrectories as they won't be skipped regarding the input
        pattern.

        '''
        base = self.resolve()
        pattern1 = os.path.join(base, pattern)
        for p in self._fs.list(base, recursive=True):
            if fnmatch.fnmatch(os.path.join(base, p), pattern1):
                yield p

    def stat(self):
        return self._fs.stat(self.resolve())

    def unlink(self, missing_ok=False):
        return self._fs.remove(self.resolve())

    def rename(self, target):
        raise NotImplementedError("rename() is unsupported on this system")

    def read_text(self):
        with self.open() as fp:
            return fp.read()

    def read_bytes(self):
        with self.open(mode='rb') as fp:
            return fp.read()

    def write_text(self, data):
        with self.open(mode='w') as fp:
            return fp.write(data)

    def write_bytes(self, data):
        view = memoryview(data)
        with self.open(mode='wb') as fp:
            return fp.write(data)
