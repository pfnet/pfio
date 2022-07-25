import filecmp
import multiprocessing
import os
import random
import tempfile
import zipfile

from pfio.cache import MultiprocessSparseFileCache, SparseFileCache
from pfio.testing import ZipForTest


def test_sparse_file_cache():

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "test.zip")
        _ = ZipForTest(filepath)

        stat = os.stat(filepath)

        size = stat.st_size
        with open(filepath, 'rb') as xfp:

            with SparseFileCache(xfp, size) as fp:

                fp.seek(26)
                data = fp.read(17)

                w = 17
                for _ in range(100):
                    offset = random.randint(0, size-w)
                    fp.seek(offset)
                    data = fp.read(w)
                    # print("ranges:", len(fp.ranges))
                    # print(w, len(data))
                    assert w == len(data)

                fp.seek(0)


def test_sparse_file_cache2():

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "test.zip")
        _ = ZipForTest(filepath)

        stat = os.stat(filepath)

        preserve = os.path.join(tempdir, "cache.file")

        size = stat.st_size
        with open(filepath, 'rb') as xfp, open(filepath, 'rb') as yfp:

            with SparseFileCache(xfp, size) as fp:

                fp.seek(26)
                # print('seek done:', fp.pos, xfp.tell())
                data = fp.read(17)
                # print(len(data))

                w = 17
                for _ in range(100):
                    offset = random.randint(0, size-w)
                    fp.seek(offset)
                    assert fp.pos == xfp.tell()
                    buf = fp.read(w)
                    # print("><", offset, offset+w, fp.pos, xfp.tell())
                    assert w == len(buf)
                    # print("ranges:", len(fp.ranges))
                    # print(w, len(data))

                    for d, r in fp._read_all_cache():
                        # print(r)
                        cache = os.pread(yfp.fileno(), r.length, r.start)
                        if r.cached:
                            assert cache == d

                fp.seek(26)
                # print("><", fp.pos, xfp.tell())
                data2 = fp.read(17)
                assert data == data2
                # print("lst read:", len(fp.read(size)))

                fp.preserve(preserve)

        filecmp.cmp(filepath, preserve)


def test_sparse_file_cache_mp():

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "test.zip")

        n_workers = 8

        # 1000 1024-byte files
        data = {'dir': {
            'file-{}'.format(i): b'x' * 1024
            for i in range(1000)}
        }
        _ = ZipForTest(filepath, data)

        stat = os.stat(filepath)
        size = stat.st_size

        def check(a, fp):
            try:
                with open(filepath, 'rb') as xfp:
                    w = 17
                    fp.seek(26)
                    # print('seek done:', fp.pos, xfp.tell())
                    data = fp.read(17)
                    # print(len(data))
                    xfp.seek(26)
                    data2 = xfp.read(17)
                    assert data == data2

                    for _ in range(100000):
                        offset = random.randint(0, size-w)
                        fp.seek(offset)
                        xfp.seek(offset)
                        assert fp.pos == xfp.tell()
                        buf = fp.read(w)
                        # print("><", offset, offset+w, fp.pos, xfp.tell())
                        assert w == len(buf)

                    fp.seek(26)
                    # print("><", fp.pos, xfp.tell())
                    data2 = fp.read(17)
                    assert data == data2
                    # print("lst read:", len(fp.read(size)))
                    q.put((True, None))
            except Exception as e:
                # print(cfp, e)
                q.put((False, e))

        with open(filepath, 'rb') as xfp:
            with MultiprocessSparseFileCache(xfp, size) as cfp0:

                q = multiprocessing.Queue()

                ps = [multiprocessing.Process(target=check, args=(q, cfp0))
                      for worker_idx in range(n_workers)]
                for p in ps:
                    p.start()
                for p in ps:
                    p.join()
                    ok, e = q.get()
                    assert ok, str(e)


def test_sparse_cache_zip():

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "test.zip")
        z = ZipForTest(filepath)

        stat = os.stat(filepath)

        size = stat.st_size
        with open(filepath, 'rb') as xfp:
            with SparseFileCache(xfp, size) as cfp:
                with zipfile.ZipFile(cfp, 'r') as zfp:

                    assert zfp.testzip() is None
                    assert 2 == len(zfp.namelist())
                    with zfp.open("file", 'r') as fp:
                        assert z.content("file") == fp.read()

                    with zfp.open("dir/f", "r") as fp:
                        assert b'bar' == fp.read()
