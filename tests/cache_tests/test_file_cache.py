from pfio.cache import FileCache
from pfio.cache import MultiprocessFileCache
import os
import tempfile

import pytest


def test_enospc(monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(28, "No space left on device")
        raise ose
    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with FileCache(10) as cache:
            i = 2
            with pytest.warns(RuntimeWarning):
                cache.put(i, str(i))


def test_enoent(monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(2, "No such file or directory")
        raise ose
    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with FileCache(10) as cache:

            with pytest.raises(OSError):
                cache.put(4, str(4))


def test_preservation():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        cache.preserve('preserved')

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        # Imitating a new process, fresh load
        cache2 = FileCache(10, dir=d, do_pickle=True)

        cache2.preload('preserved')
        for i in range(10):
            assert str(i) == cache2.get(i)


def test_preservation_interoperability():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        cache.preserve('preserved')

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        cache2 = MultiprocessFileCache(10, dir=d, do_pickle=True)

        cache2.preload('preserved')
        for i in range(10):
            assert str(i) == cache2.get(i)
