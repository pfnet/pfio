import fnmatch
import os
import warnings
from pathlib import PurePath

from .fs import FS, _thread_local
from .local import Local


class Path:
    sep = '/'

    def __init__(self, *args, fs=None, root=None):
        '''

        Empty argument indicates current directory in fs, fs.cwd

        '''
        self._fs = fs
        if fs is not None:
            warnings.Warn("argument fs for Path is deprecated",
                          DeprecationWarning)
            assert isinstance(self._fs, FS)

        sep = self.sep
        if root:
            if not root.startswith(self.sep):
                raise ValueError("root must start with a separator.")
            if root == self.sep:
                args = [self.sep] + args
            else:
                args = [self.sep, root[len(self.sep):]] + args

        # Construct parts
        parts = []
        for argv in args:
            p = argv
            if p.startswith(sep):
                parts = [sep]
                p = p[len(sep):]
            if len(p) > 0:
                parts += [q for q in p.split(sep) if q not in ("", ".")]

        # Check
        assert "" not in parts
        if parts and parts[0] == self.sep:
            assert all(self.sep not in p for p in parts[1:])
        else:
            assert all(self.sep not in p for p in parts)

        self._parts = parts

    @property
    def name(self):
        if not self._parts:
            return ""
        if len(self._parts) == 1 and self._parts[0] == self.sep:
            return ""
        return os.path.normpath(self._parts[-1])

    @property
    def suffix(self):
        return os.path.splitext(self.resolve())[1]

    def with_suffix(self, ext):
        assert ext.startswith('.')
        name = self.name
        if name == "":
            raise ValueError(f"{self} has an empty name")
        base, _ = os.path.splitext(name)
        return self.parent / f"{base}{ext}"

    @property
    def parent(self):
        if not self._parts:
            return self

        if self._parts[0] == self.sep and len(self._parts) == 1:
            return self

        return Path(*(self._parts[:-1]), fs=self._fs)

    @property
    def parts(self):
        return tuple(self._parts)

    @property
    def root(self):
        if self._parts and self._parts[0] == self.sep:
            return self.sep
        return ""

    def __rtruediv__(self, a):
        if isinstance(a, str):
            lhs = Path(a, fs=self._fs)
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

        p = Path(*parts, fs=self._fs)
        return p

    @classmethod
    def cwd(cls):
        raise NotImplementedError("cwd() is unsupported on this system")

    @classmethod
    def home(cls):
        raise NotImplementedError("home() is unsupported on this system")

    def exists(self):
        fs = self._resolve_fs()
        return fs.exists(str(self.resolve()))

    def is_absolute(self):
        return self._parts and self._parts[0] == self.sep

    def __repr__(self):
        fs = self._resolve_fs()
        return "{}[{}:{}]".format(self.__class__.__name__,
                                  fs.__class__.__name__,
                                  str(self))

    def __str__(self):
        if not self._parts:
            return "."
        if self._parts[0] == self.sep:
            return self.sep + self.sep.join(self._parts[1:])
        return self.sep.join(self._parts)

    def __fspath__(self):
        return str(self)

    def __lt__(self, other):
        return str(self) < str(other)

    def resolve(self, strict=True):
        if '..' in self._parts or '.' in self._parts:
            raise RuntimeError("TODO")
        if self._fs is None:
            return Path(*self._parts, fs=None)

        parts = [self._fs.cwd] + self._parts
        fs = self._fs._newfs('')
        return Path(*parts, fs=fs)

    def samefile(self, other):
        # TODO: Compare self._fs
        return str(self.resolve()) == str(other.resolve())

    def touch(self):
        fs = self._resolve_fs()
        with fs.open(self.resolve(), 'wb') as fp:
            fp.write(b'')

    def open(self, mode='r', **kwargs):
        fs = self._resolve_fs()
        # TODO: handle or warn on ignoring these keyword arguments
        # buffering=-1, encoding=None, errors=None, newline=None):
        return fs.open(self.resolve(), mode)

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        fs = self._resolve_fs()
        if parents:
            return fs.makedirs(self.resolve(), mode, exist_ok)

        try:
            fs.mkdir(self.resolve(), mode)
        except FileExistsError as e:
            if not exist_ok:
                raise e
            else:
                pass

    def is_dir(self):
        fs = self._resolve_fs()
        return fs.isdir(self.resolve())

    def is_file(self):
        fs = self._resolve_fs()
        return not fs.isdir(self.resolve())

    def iterdir(self):
        return self.glob("*")

    def glob(self, pattern):
        '''glob

        It may be slow depending on the filesystem type.
        '''
        # Try specialized implementation
        with self._as_fs() as fs:
            try:
                generator = fs.glob(pattern)
            except NotImplementedError:
                pass
            else:
                return (self / f for f in generator)

        # Fall back to slower generic implementation
        return self._glob_generic(pattern)

    def _glob_generic(self, pattern):
        '''Slow and generic implementation of glob.'''
        base = self.resolve()
        base_parts = base._parts
        pattern_parts = (base / pattern)._parts

        visited_prefixes = set()
        fs = self._resolve_fs()
        for p in fs.list(base, recursive=True):
            parent = p

            while (i := parent.rfind(self.sep)) != -1:
                parent = parent[:i]
                if parent in visited_prefixes:
                    continue
                visited_prefixes.add(parent)

                target_parts = (base / parent)._parts

                if _test_glob_by_parts(target_parts, pattern_parts):
                    yield self / parent

            target_parts = base_parts + p.split("/")

            if _test_glob_by_parts(target_parts, pattern_parts):
                yield self / p

    def stat(self):
        fs = self._resolve_fs()
        return fs.stat(self.resolve())

    def unlink(self, missing_ok=False):
        fs = self._resolve_fs()
        return fs.remove(self.resolve())

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

    def _resolve_fs(self):
        if self._fs:
            return self._fs

        fs = getattr(_thread_local, "_pfio_fs", None)
        if fs is not None:
            return fs

        return Local()

    def _as_fs(self):
        fs = self._resolve_fs()
        return fs._newfs(str(self.resolve()))


def _test_glob_by_parts(target_parts, pattern_parts):
    target_parts = target_parts[:]
    pattern_parts = pattern_parts[:]
    while True:
        if len(target_parts) == 0 and len(pattern_parts) == 0:
            return True

        if len(target_parts) == 0 or len(pattern_parts) == 0:
            return False

        p = pattern_parts.pop(0)
        if p == "**":
            for i in range(0, len(target_parts)):
                if _test_glob_by_parts(target_parts[i:], pattern_parts):
                    return True
            return False

        t = target_parts.pop(0)
        if not fnmatch.fnmatch(t, p):
            return False
