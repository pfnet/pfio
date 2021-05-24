import os
import random
import string
import subprocess
from unittest import mock
from zipfile import ZipFile


class ZipForTest:
    def __init__(self, destfile, data=None):
        if data is None:
            self.data = dict(
                file=b"foo",
                dir=dict(
                    f=b"bar"
                )
            )
        else:
            self.data = data

        self._make_zip(destfile)
        self.destfile = destfile

    def content(self, path):
        d = self.data

        for node in path.split(os.path.sep):
            d = d.get(node)
            if not isinstance(d, dict):
                return d

    def _make_zip(self, destfile):
        with ZipFile(destfile, "w") as z:
            stack = []
            self._write_zip_contents(z, stack, self.data)

    def _write_zip_contents(self, z, stack, data):
        for k in data:
            if isinstance(data[k], dict):
                self._write_zip_contents(z, stack+[k], data[k])
            else:
                path = os.path.join(*stack, k)
                with z.open(path, 'w') as fp:
                    fp.write(data[k])


def make_zip(zipfilename, root_dir, base_dir):
    pwd = os.getcwd()
    with ZipFile(zipfilename, "w") as f:
        try:
            os.chdir(root_dir)
            for root, dirs, filenames in os.walk(base_dir):
                for _dir in dirs:
                    path = os.path.normpath(os.path.join(root, _dir))
                    f.write(path)
                for _file in filenames:
                    path = os.path.normpath(os.path.join(root, _file))
                    f.write(path)
        finally:
            os.chdir(pwd)


def make_random_str(n):
    return ''.join([random.choice(string.ascii_letters + string.digits)
                    for i in range(n)])


def randstring(length=16):
    letters = string.ascii_letters + string.digits
    return (''.join(random.choice(letters) for _ in range(length)))


def patch_subprocess(stdout, stderr=b''):
    def decorator(f):
        def wrapper(*args, **kwargs):
            orig_method = subprocess.run
            try:
                cp = subprocess.CompletedProcess(args='hoge', returncode=0)
                cp.stdout = stdout
                cp.stderr = stderr
                subprocess.run = mock.create_autospec(subprocess.run,
                                                      return_value=cp)
                return f(*args, **kwargs)
            finally:
                subprocess.run = orig_method

        return wrapper
    return decorator
