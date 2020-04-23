import hashlib
import pickle
import random

import pytest
from pfio.cache import NaiveCache, FileCache


@pytest.mark.parametrize("test_class", [NaiveCache, FileCache])
@pytest.mark.parametrize("mt_safe", [True, False])
@pytest.mark.parametrize("do_pickle", [True, False])
@pytest.mark.parametrize("do_shuffle", [True, False])
def test_cache(test_class, mt_safe, do_pickle, do_shuffle):
    length = l = 1024
    cache = test_class(l, multithread_safe=mt_safe, do_pickle=do_pickle)
    assert not cache.multiprocess_safe
    assert cache.multithread_safe == mt_safe

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


def _getbin(i):
    m = hashlib.md5()
    m.update(str(i).encode())
    # blob = b''
    digest = m.digest()
    size = 1024 * 16
    # 1024 * 16 = 16KB / 1024*1024 * 16 = 16MB
    blob = [digest * size][0]
    return blob


@pytest.mark.parametrize("test_class", [NaiveCache, FileCache])
@pytest.mark.parametrize("mt_safe", [True, False])
@pytest.mark.parametrize("length", [
    pytest.param(-1, marks=pytest.mark.xfail),
    pytest.param(0, marks=pytest.mark.xfail),
    1, 20, 100, 1000])
def test_cache_blob(test_class, mt_safe, length):
    l = length
    cache = test_class(l, multithread_safe=mt_safe, do_pickle=False)
    assert not cache.multiprocess_safe
    assert cache.multithread_safe == mt_safe

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
