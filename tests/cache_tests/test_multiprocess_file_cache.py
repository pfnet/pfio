from pfio.cache import MultiprocessFileCache
import multiprocessing
import os
import pickle
import tempfile

import pytest


def test_pickable():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)
        try:
            pickle.dumps(cache)
        except TypeError:
            pytest.fail("Unpicklabe Pickle fails")

        cache.close()


def test_cleanup():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        assert len(os.listdir(d)) == 2

        cache.close()

        assert len(os.listdir(d)) == 0


def test_cleanup_subprocess():
    def child(c):
        c.close()

    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)
        p = multiprocessing.Process(target=child, args=(cache,))
        p.start()
        p.join()

        # Calling close in the subprocess should not
        # delete the cache files
        assert len(os.listdir(d)) == 2

        cache.close()

        assert len(os.listdir(d)) == 0


def test_preservation():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        cache.preserve('preserved')

        cache.close()

        # Imitating a new process, fresh load
        cache2 = MultiprocessFileCache(10, dir=d, do_pickle=True)

        cache2.preload('preserved')
        for i in range(10):
            assert str(i) == cache2.get(i)

        cache2.close()

        # No temporary cache file should remain,
        # and the preserved cache should be kept.
        assert os.listdir(d) == ['preserved.cached', 'preserved.cachei']


def test_preservation_error_already_exists():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        cache.preserve('preserved')

        with pytest.raises(ValueError):
            cache.preserve('preserved')

        cache.close()


def test_preload_error_not_found():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        with pytest.raises(ValueError):
            cache.preload('preserved')

        cache.close()


def test_enospc(monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(28, "No space left on device")
        raise ose

    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with MultiprocessFileCache(10) as cache:
            i = 2
            with pytest.warns(RuntimeWarning):
                cache.put(i, str(i))


def test_enoent(monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(2, "No such file or directory")
        raise ose

    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with MultiprocessFileCache(10) as cache:

            with pytest.raises(OSError):

                cache.put(4, str(4))
