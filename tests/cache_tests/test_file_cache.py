from pfio.cache import FileCache
from pfio.cache import MultiprocessFileCache
import tempfile


def test_preservation_interoperability():
    with tempfile.TemporaryDirectory() as d:
        cache = FileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        assert cache.preserve('preserved') is True

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        cache2 = MultiprocessFileCache(10, dir=d, do_pickle=True)

        assert cache2.preload('preserved') is True
        for i in range(10):
            assert str(i) == cache2.get(i)
