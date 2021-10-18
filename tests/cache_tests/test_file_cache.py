import tempfile

import pytest

import pfio
from pfio.cache import FileCache, MultiprocessFileCache, ReadOnlyFileCache
from pfio.testing import patch_subprocess


def test_preservation_interoperability():
    length = 2345
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(length, dir=d, do_pickle=True)

        for i in range(length):
            cache.put(i, str(i))

        assert cache.preserve('preserved') is True

        for i in range(length):
            assert str(i) == cache.get(i)

        cache.close()

        with MultiprocessFileCache(length, dir=d, do_pickle=True) as cache2:
            assert cache2.preload('preserved')
            for i in range(length):
                assert str(i) == cache2.get(i)

        with ReadOnlyFileCache(length, dir=d, do_pickle=True) as roc:
            with pytest.raises(Exception):
                roc.preserve('hoge')
            with pytest.raises(Exception):
                roc.put(234, b'hoge')

            assert roc.frozen
            assert roc.closed

            assert roc.preload('preserved')
            for i in range(length):
                assert str(i) == roc.get(i)

            assert not roc.closed


@patch_subprocess(b'ext3/ext4')
def test_no_nfs():
    FileCache(dir='var', length=214)

    MultiprocessFileCache(dir='var', length=234)


@patch_subprocess(b'nfs')
def test_nfs():
    with pytest.raises(ValueError):
        FileCache(dir='var', length=214)

    with pytest.raises(ValueError):
        MultiprocessFileCache(dir='var', length=214)

    pfio.cache.file_cache._FORCE_LOCAL = False

    FileCache(dir='var', length=214)
    MultiprocessFileCache(dir='var', length=214)
