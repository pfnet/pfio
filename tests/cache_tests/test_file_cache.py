import tempfile

import pytest
from parameterized import parameterized

import pfio
from pfio.cache import FileCache, MultiprocessFileCache
from pfio.testing import patch_subprocess


@parameterized.expand([(True,), (False,)])
def test_preservation_interoperability(o_direct):
    with tempfile.TemporaryDirectory(dir='.') as d:
        cache = FileCache(10, dir=d, do_pickle=True,
                          o_direct=o_direct)

        for i in range(10):
            cache.put(i, str(i))

        assert cache.preserve('preserved') is True

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        cache2 = MultiprocessFileCache(10, dir=d, do_pickle=True,
                                       o_direct=o_direct)

        assert cache2.preload('preserved') is True
        for i in range(10):
            assert str(i) == cache2.get(i)


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
