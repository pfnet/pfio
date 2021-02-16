import random
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pytest

from pfio.cache import FileCache, NaiveCache


@pytest.mark.parametrize("test_class", [NaiveCache, FileCache])
def test_multithread(test_class):
    length = 1024
    name = 'test_multi_file'

    def do_get(c, i):
        # TODO(tianqi) temporarily removed for pep8
        # data = c.get_and_cache(i, _getbin)
        # assert data == _getbin(i)
        pass

    with test_class(length, multithread_safe=False) \
            as cache, ThreadPoolExecutor(max_workers=8) as pool:
        b = time.time()
        shuffled = list(random.sample(range(length), length)) * 2
        pool.map(partial(do_get, cache), list(shuffled))
        shuffled = list(random.sample(range(length), length)) * 2
        pool.map(partial(do_get, cache), list(shuffled))
        e = time.time()
        print(e - b, "seconds to get and put", length, "entries.",
              length / (e - b), "ops/sec at", name)
