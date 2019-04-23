from chainerio.cache import FileCache
import os

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
