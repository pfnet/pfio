import hashlib
import os
import pickle
import random
import tempfile

import numpy as np
import pytest
from pfio.cache import FileCache, MultiprocessFileCache, NaiveCache


def make_cache(test_class, mt_safe, do_pickle, length,
               cache_size_limit=None):
    if test_class == NaiveCache:
        assert cache_size_limit is None, \
            "NaiveCache doesn't support cache size limit"
        cache = test_class(length, multithread_safe=mt_safe,
                           do_pickle=do_pickle)
        assert not cache.multiprocess_safe
        assert cache.multithread_safe == mt_safe

    elif test_class == FileCache:
        cache = test_class(length, multithread_safe=mt_safe,
                           cache_size_limit=cache_size_limit,
                           do_pickle=do_pickle)
        assert not cache.multiprocess_safe
        assert cache.multithread_safe == mt_safe

    elif test_class == MultiprocessFileCache:
        cache = test_class(length, cache_size_limit=cache_size_limit,
                           do_pickle=do_pickle,)
        assert cache.multiprocess_safe
        assert cache.multithread_safe

    else:
        assert False

    return cache


@pytest.mark.parametrize("test_class", [NaiveCache, FileCache,
                                        MultiprocessFileCache])
@pytest.mark.parametrize("mt_safe", [True, False])
@pytest.mark.parametrize("do_pickle", [True, False])
@pytest.mark.parametrize("do_shuffle", [True, False])
def test_cache(test_class, mt_safe, do_pickle, do_shuffle):
    length = l = 1024
    cache = make_cache(test_class, mt_safe, do_pickle, l)

    if not do_pickle:
        def getter(x):
            return pickle.dumps(x * 2)
    else:
        def getter(x):
            return x * 2

    if do_shuffle:
        shuffled = list(random.sample(range(length), length)) * 2
    else:
        shuffled = list(range(length)) * 2

    for i in shuffled:
        j = i % l
        data = cache.get_and_cache(j, getter)
        if not do_pickle:
            data = pickle.loads(data)
        assert j * 2 == data

    for i in shuffled:
        j = i % l
        data = cache.get(j)
        assert data is not None
        if not do_pickle:
            data = pickle.loads(data)
        assert j * 2 == data


@pytest.mark.parametrize("test_class", [NaiveCache, FileCache,
                                        MultiprocessFileCache])
@pytest.mark.parametrize("mt_safe", [True, False])
@pytest.mark.parametrize("do_shuffle", [True, False])
def test_cache_numpy(test_class, mt_safe, do_shuffle):
    length = l = 1024
    cache = make_cache(test_class, mt_safe, True, l)
    arr_list = np.random.rand(l, 3, 4)

    def getter(i):
        return arr_list[i]
    if do_shuffle:
        shuffled = list(random.sample(range(length), length)) * 2
    else:
        shuffled = list(range(length)) * 2

    for i in shuffled:
        j = i % l
        data = cache.get_and_cache(j, getter)
        assert np.array_equal(arr_list[j], data)

    for i in shuffled:
        j = i % l
        data = cache.get(j)
        assert data is not None
        assert np.array_equal(arr_list[j], data)


def _getbin(i):
    m = hashlib.md5()
    m.update(str(i).encode())
    # blob = b''
    digest = m.digest()
    size = 1024 * 16
    # 1024 * 16 = 16KB / 1024*1024 * 16 = 16MB
    blob = [digest * size][0]
    return blob


@pytest.mark.parametrize("test_class", [NaiveCache, FileCache,
                                        MultiprocessFileCache])
@pytest.mark.parametrize("mt_safe", [True, False])
@pytest.mark.parametrize("length", [
    pytest.param(-1, marks=pytest.mark.xfail),
    pytest.param(0, marks=pytest.mark.xfail),
    1, 20, 100, 1000])
def test_cache_blob(test_class, mt_safe, length):
    l = length
    cache = make_cache(test_class, mt_safe, False, l)

    for i in range(l):

        def getter(x):
            return pickle.dumps(x)

        data = cache.get_and_cache(i, _getbin)
        assert _getbin(i) == data

    for i in range(l):
        data = cache.get(i)
        assert data is not None
        assert _getbin(i) == data

    for i in range(l):
        data = cache.get(i)
        assert data is not None
        assert _getbin(i) == data


def test_index_range_naive():
    l = 10
    cache = make_cache(NaiveCache, True, False, l)

    # Index check for Put

    cache.put(-1, pickle.dumps(9 ** 2))

    for i in range(l - 1):
        cache.put(i, pickle.dumps(i * 2))

    with pytest.raises(IndexError):
        cache.put(l, pickle.dumps('too large'))

    # Index check for Get

    with pytest.raises(IndexError):
        cache.get(l)

    assert pickle.loads(cache.get(-1)) == 9 ** 2
    assert pickle.loads(cache.get(9)) == 9 ** 2


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_index_range_get(test_class):
    l = 10
    cache = make_cache(test_class, True, False, l)

    for i in range(l):
        cache.put(i, pickle.dumps(i * 2))

    with pytest.raises(IndexError):
        cache.get(-1)

    with pytest.raises(IndexError):
        cache.get(l)


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_index_range_put(test_class):
    l = 10
    cache = make_cache(test_class, True, False, l)

    with pytest.raises(IndexError):
        cache.put(-1, pickle.dumps('negative'))

    with pytest.raises(IndexError):
        cache.put(l, pickle.dumps('too large'))


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_cache_limit_invalid_limits(test_class):
    with pytest.raises(ValueError):
        make_cache(test_class, True, False, 10, cache_size_limit=-1)
    with pytest.raises(ValueError):
        make_cache(test_class, True, False, 10, cache_size_limit='10')


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_cache_limit_ok(test_class):
    sample_size = 10
    l = 20

    # index part = 8x2xl = 320(bytes)
    # data part = 100(bytes)
    cache = make_cache(test_class, True, False, l,
                       cache_size_limit=(320 + 100))
    assert len(cache) == l

    # To make sure the order of data to arrive
    # has nothing to do with the size limitation logic
    idxs = list(range(l))
    random.shuffle(idxs)

    # It accepts the data until reaching to size limit
    data = b'x' * sample_size
    for i in idxs[:10]:
        cache.put(i, data)

        # To make sure reading the data while putting data
        # doesn't interfere
        j = random.randrange(l)
        cache.get(j)

    # More data cannot be accepted (though not error)
    for i in idxs[10:]:
        cache.put(i, data)

    # Data already cached should remain
    for i in idxs[:10]:
        assert cache.get(i) == data

    # Confirm no other data is cached
    for i in idxs[10:]:
        assert cache.get(i) is None


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_cache_limit_auto_freeze(test_class):
    l = 10

    # index part = 8x2x10(bytes)
    # data part = 120(bytes)
    cache = make_cache(test_class, True, False, l,
                       cache_size_limit=(160 + 120))

    data_50bytes = b'x' * 50
    cache.put(0, data_50bytes)
    cache.put(1, data_50bytes)
    assert not cache.put(2, data_50bytes)   # Here it reaches to the limit

    assert cache.get(0) == data_50bytes
    assert cache.get(1) == data_50bytes

    # This cache is now frozen; no longer accepts further put,
    # even if the next data is small enough for the remained size.
    data_20bytes = b'y' * 20
    assert not cache.put(3, data_20bytes)
    assert cache.get(3) is None


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_preservation(test_class):
    with tempfile.TemporaryDirectory() as d:
        cache = test_class(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        assert cache.preserve('preserved') is True

        for i in range(10):
            assert str(i) == cache.get(i)

        cache.close()

        # Imitating a new process, fresh load
        cache2 = FileCache(10, dir=d, do_pickle=True)

        assert cache2.preload('preserved') is True
        for i in range(10):
            assert str(i) == cache2.get(i)


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_preservation_multiple_times(test_class):
    with tempfile.TemporaryDirectory() as d:
        cache = test_class(1, dir=d, do_pickle=True)
        cache.put(0, 'hello')
        cache.preserve('preserved1')
        cache.preserve('preserved2')

        cache2 = test_class(1, dir=d, do_pickle=True)
        cache2.preload('preserved1')
        assert cache2.get(0) == 'hello'

        cache3 = test_class(1, dir=d, do_pickle=True)
        cache3.preload('preserved2')
        assert cache3.get(0) == 'hello'


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_preload_and_preserve(test_class):
    with tempfile.TemporaryDirectory() as d:
        cache = test_class(1, dir=d, do_pickle=True)
        cache.put(0, 'hello')
        cache.preserve('preserved')

        cache2 = test_class(1, dir=d, do_pickle=True)
        cache2.preload('preserved')
        cache2.preserve('preserved2')

        cache3 = test_class(1, dir=d, do_pickle=True)
        cache3.preload('preserved2')
        assert cache3.get(0) == 'hello'


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_preservation_error_already_exists(test_class):
    with tempfile.TemporaryDirectory() as d:
        cache = test_class(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        assert cache.preserve('preserved') is True

        assert cache.preserve('preserved') is False

        cache.close()


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_preservation_overwrite(test_class):
    with tempfile.TemporaryDirectory() as d:
        cache = test_class(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        # Create a dummy file
        with open(os.path.join(d, 'preserved.cachei'), 'wt') as f:
            f.write('hello')

        cache.preserve('preserved', overwrite=True)

        cache2 = test_class(10, dir=d, do_pickle=True)

        assert cache2.preload('preserved') is True
        for i in range(10):
            assert str(i) == cache2.get(i)


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_enospc(test_class, monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(28, "No space left on device")
        raise ose
    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with test_class(10) as cache:
            i = 2
            with pytest.warns(RuntimeWarning):
                cache.put(i, str(i))


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_enoent(test_class, monkeypatch):
    def mock_pread(_fd, _buf, _offset):
        ose = OSError(2, "No such file or directory")
        raise ose

    with monkeypatch.context() as m:
        m.setattr(os, 'pread', mock_pread)

        with test_class(10) as cache:

            with pytest.raises(OSError):

                cache.put(4, str(4))


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_preload_error_not_found(test_class):
    with tempfile.TemporaryDirectory() as d:
        cache = test_class(10, dir=d, do_pickle=True)

        assert cache.preload('preserved') is False

        cache.close()


@pytest.mark.parametrize("test_class", [FileCache, MultiprocessFileCache])
def test_default_cache_path(test_class):
    orig = os.getenv('XDG_CACHE_HOME')

    os.environ['XDG_CACHE_HOME'] = "/tmp/pfio-cache"

    try:
        with test_class(16) as c:
            assert "/tmp/pfio-cache/pfio" == c.dir

        os.environ['XDG_CACHE_HOME'] = ''

        with test_class(16) as c:
            path = os.path.join(os.getenv('HOME'), '.cache', 'pfio')
            assert path == c.dir

    finally:
        if orig is not None:
            os.environ['XDG_CACHE_HOME'] = orig
