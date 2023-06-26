import multiprocessing
import os
import pickle
import tempfile

import numpy as np
import pytest

from pfio.cache import FileCache, MultiprocessFileCache


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
    def child(c):
        c.close()

    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)
        p = multiprocessing.Process(target=child, args=(cache,))
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
    n_samples_per_worker = 1024
    sample_size = 8192

    def child(cache, worker_idx):
        for i in range(n_samples_per_worker):
            sample_idx = worker_idx * n_samples_per_worker + i
            data = np.array([sample_idx] * sample_size, dtype=np.int32)
            cache.put(sample_idx, data)

    with tempfile.TemporaryDirectory() as d:
        with MultiprocessFileCache(n_samples_per_worker * n_workers,
                                   dir=d, do_pickle=True) as cache:

            # Add tons of data into the cache in parallel
            ps = [multiprocessing.Process(target=child, args=(cache, worker_idx))  # NOQA
                  for worker_idx in range(n_workers)]
            for p in ps:
                p.start()
            for p in ps:
                p.join()

            # Get each sample from the cache and check the content
            for sample_idx in range(n_workers * n_samples_per_worker):
                data = cache.get(sample_idx)
                expected = np.array([sample_idx] * sample_size, dtype=np.int32)
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


def test_preserve_error_subprocess():
    pipe_recv, pipe_send = multiprocessing.Pipe(False)

    def child(c, pipe):
        try:
            c.preserve('preserved')
        except Exception as e:
            pipe.send(pickle.dumps(e))
        finally:
            pipe.close()

    with tempfile.TemporaryDirectory() as d:
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)

        for i in range(10):
            cache.put(i, str(i))

        # Run preservation in the subprocess
        p = multiprocessing.Process(target=child, args=(cache, pipe_send))
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


def test_preload_error_subprocess():
    pipe_recv, pipe_send = multiprocessing.Pipe(False)

    def child(c, pipe):
        try:
            c.preload('preserved')
        except Exception as e:
            pipe.send(pickle.dumps(e))
        finally:
            pipe.close()

    with tempfile.TemporaryDirectory() as d:
        # Run preload in the subprocess
        cache = MultiprocessFileCache(10, dir=d, do_pickle=True)
        p = multiprocessing.Process(target=child, args=(cache, pipe_send))
        p.start()
        p.join()
        cache.close()

        e = pickle.loads(pipe_recv.recv())
        assert isinstance(e, RuntimeError)
