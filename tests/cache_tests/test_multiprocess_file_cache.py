import multiprocessing
import os
import pickle
import tempfile

import numpy as np
import pytest

from pfio.cache import FileCache, MultiprocessFileCache

_CONSISTENCY_N_SAMPLES_PER_WORKER = 1024
_CONSISTENCY_SAMPLE_SIZE = 8192


def _cleanup_subprocess_child(c):
    c.close()


def _consistency_child(cache, worker_idx):
    for i in range(_CONSISTENCY_N_SAMPLES_PER_WORKER):
        sample_idx = worker_idx * _CONSISTENCY_N_SAMPLES_PER_WORKER + i
        data = np.array([sample_idx] * _CONSISTENCY_SAMPLE_SIZE, dtype=np.int32)
        cache.put(sample_idx, data)


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

        assert len(os.listdir(d)) == 1

        cache.close()

        assert len(os.listdir(d)) == 0


def test_cleanup_subprocess():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)
        p = multiprocessing.Process(target=_cleanup_subprocess_child, args=(cache,))
        p.start()
        p.join()

        # Calling close in the subprocess should not
        # delete the cache files
        assert len(os.listdir(d)) == 1

        cache.close()

        assert len(os.listdir(d)) == 0


def test_multiprocess_consistency():
    # Condition: 32k samples (8k*4bytes each) cached by 32 workers.
    # Each sample is an array of repeated sample index.
    # ie. k-th sample is np.array([k, k, k, ..., k], dtype=np.int32)
    # 32 worker processes simultaneously create such data and insert them into
    # a single cache, and we check if the data can be correctly recovered.
    n_workers = 32

    with tempfile.TemporaryDirectory() as d:
        with MultiprocessFileCache(_CONSISTENCY_N_SAMPLES_PER_WORKER * n_workers,
                                   dir=d, do_pickle=True) as cache:

            # Add tons of data into the cache in parallel
            ps = [multiprocessing.Process(target=_consistency_child, args=(cache, worker_idx))  # NOQA
                  for worker_idx in range(n_workers)]
            for p in ps:
                p.start()
            for p in ps:
                p.join()

            # Get each sample from the cache and check the content
            for sample_idx in range(n_workers * _CONSISTENCY_N_SAMPLES_PER_WORKER):
                data = cache.get(sample_idx)
                expected = np.array([sample_idx] * _CONSISTENCY_SAMPLE_SIZE, dtype=np.int32)
                assert (data == expected).all()


def test_preservation_interoperability():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        assert cache.preserve('preserved') is True

        cache.close()

        cache2 = FileCache(10, dir=d, do_pickle=True)

        assert cache2.preload('preserved') is True
        for i in range(10):
            assert str(i) == cache2.get(i)

        cache2.close()


def _preserve_error_subprocess_child(c, pipe):
    try:
        c.preserve('preserved')
    except Exception as e:
        pipe.send(pickle.dumps(e))
    finally:
        pipe.close()


def test_preserve_error_subprocess():
    pipe_recv, pipe_send = multiprocessing.Pipe(False)

    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        # Run preservation in the subprocess
        p = multiprocessing.Process(target=_preserve_error_subprocess_child, args=(cache, pipe_send))
        p.start()
        p.join()
        cache.close()

        e = pickle.loads(pipe_recv.recv())
        assert isinstance(e, RuntimeError)


def test_preload_error_not_found():
    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        assert cache.preload('preserved') is False

        cache.close()


def _preload_error_subprocess_child(c, pipe):
    try:
        c.preload('preserved')
    except Exception as e:
        pipe.send(pickle.dumps(e))
    finally:
        pipe.close()


def test_preload_error_subprocess():
    pipe_recv, pipe_send = multiprocessing.Pipe(False)

    with tempfile.TemporaryDirectory() as d:
        # Run preload in the subprocess
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)
        p = multiprocessing.Process(target=_preload_error_subprocess_child, args=(cache, pipe_send))
        p.start()
        p.join()
        cache.close()

        e = pickle.loads(pipe_recv.recv())
        assert isinstance(e, RuntimeError)
