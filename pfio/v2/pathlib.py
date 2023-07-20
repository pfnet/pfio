import fnmatch
import os
from pathlib import PurePath

from .fs import FS
from .local import Local


class Path:
    sep = '/'

    def __init__(self, *args, fs=None, root=None):
        '''

        Empty argument indicates current directory in fs, fs.cwd

        '''
        self._fs = fs if fs else Local()
        assert isinstance(self._fs, FS)

        self._root = root

        parts = []
        for argv in args:
            if argv.startswith(self.sep):
                parts = []
            parts.append(argv)
        if len(parts) == 0 and root is None:
            parts = [self._fs.cwd]

        self._parts = parts

        if self._parts and self._parts[0] == self.sep and self._root is None:
            self._parts = self._parts[1:]
            self._root = self.sep

    @property
    def name(self):
        filename = self._root if not self._parts else self._parts[-1]
        return os.path.normpath(filename)

    @property
    def suffix(self):
        return os.path.splitext(self.resolve())[1]

    def with_suffix(self, ext):
        assert ext.startswith('.')
        parts = self._parts[:]
        if parts:
            base = os.path.splitext(parts[-1])[0]
            parts[-1] = '{}{}'.format(base, ext)
            p = Path(*parts, fs=self._fs, root=self._root)
            return p

        base = os.path.splitext(self._root)[0]
        root = '{}{}'.format(base, ext)
        p = Path(*parts, fs=self._fs, root=root)
        return p

    @property
    def parent(self):
        path = str(self)
        parent = os.path.split(path)[0]
        return Path(parent, fs=self._fs, root=self._root)

    def __rtruediv__(self, a):
        if isinstance(a, str):
            lhs = Path(a, fs=self._fs, root=self._root)
        else:
            lhs = a
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
        return "{}[{}:{}]".format(self.__class__.__name__,
                                  self._fs.__class__.__name__,
                                  str(self))

    def __str__(self):

        if self._root:
            return os.path.join(self._root, *self._parts)

        return os.path.join(*self._parts)

    def __fspath__(self):
        return str(self)

    def __lt__(self, other):
        return str(self) < str(other)

    def resolve(self, strict=True):
        base = self._root if self._root else self._fs.cwd
        if '..' in self._parts or '.' in self._parts:
            raise RuntimeError("TODO")
        parts = self._parts[:]
        return Path(*parts, fs=self._fs, root=base)

    def samefile(self, other):
        return str(self.resolve()) == str(other.resolve())

    def touch(self):
        with self._fs.open(self.resolve(), 'wb') as fp:
            fp.write(b'')

    def open(self, mode='r', **kwargs):
        # TODO: handle or warn on ignoring these keyword arguments
        # buffering=-1, encoding=None, errors=None, newline=None):
        return self._fs.open(self.resolve(), mode)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        if parents:
            return self._fs.makedirs(self.resolve(), mode, exist_ok)

        try:
            self._fs.mkdir(self.resolve(), mode)
        except FileExistsError as e:
            if not exist_ok:
                raise e
            else:
                pass

    def is_dir(self):
        return self._fs.isdir(self.resolve())

    def is_file(self):
        return not self._fs.isdir(self.resolve())

    def glob(self, pattern):
        '''glob

        TODO: this implemention is not smart enough in case of large
        subdicrectories as they won't be skipped regarding the input
        pattern.

        Should return ``Path`` class as well.

        '''
        base = self.resolve()
        pattern1 = os.path.join(base, pattern)

        for p in self._fs.list(base, recursive=True):
            if fnmatch.fnmatch(os.path.join(base, p), pattern1):
                yield Path(p, fs=self._fs, root=self._root)

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
            return fp.write(view)
