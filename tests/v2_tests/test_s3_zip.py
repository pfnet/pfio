import io
import multiprocessing
import os
import shutil
import tempfile
import traceback
import zipfile

import pytest
from moto import mock_s3

import pfio
from pfio.testing import ZipForTest
from pfio.v2 import S3, Zip, from_url


@mock_s3
@pytest.mark.parametrize("local_cache", [False, True])
def test_s3_zip(local_cache):
    with tempfile.TemporaryDirectory() as d:
        zipfilename = os.path.join(d, "test.zip")
        zft = ZipForTest(zipfilename)
        bucket = "test-dummy-bucket"

        with from_url('s3://{}/'.format(bucket),
                      create_bucket=True) as s3:
            assert isinstance(s3, S3)
            with open(zipfilename, 'rb') as src,\
                    s3.open('test.zip', 'wb') as dst:
                shutil.copyfileobj(src, dst)

            with s3.open('test.zip', 'rb') as fp:
                assert zipfile.is_zipfile(fp)

        with from_url('s3://{}/test.zip'.format(bucket),
                      local_cache=local_cache) as z:
            assert isinstance(z, Zip)
            assert isinstance(z.fileobj, io.BufferedReader)

            assert zipfile.is_zipfile(z.fileobj)
            with z.open('file', 'rb') as fp:
                assert zft.content('file') == fp.read()

        with from_url('s3://{}/test.zip'.format(bucket),
                      buffering=0, local_cache=local_cache) as z:
            assert isinstance(z, Zip)
            assert 'buffering' in z.kwargs
            if not local_cache:
                assert isinstance(z.fileobj, pfio.v2.s3._ObjectReader)
            else:
                assert isinstance(z.fileobj,
                                  pfio.cache.sparse_file.CachedWrapper)

            assert zipfile.is_zipfile(z.fileobj)
            with z.open('file', 'rb') as fp:
                assert zft.content('file') == fp.read()


@mock_s3
def test_s3_zip_mp():
    with tempfile.TemporaryDirectory() as d:
        n_workers = 32
        n_samples_per_worker = 1024
        sample_size = 8192
        # 1000 1024-byte files
        data = {'dir': {
            'file-{}'.format(i): b'x' * 1024 * 17
            for i in range(sample_size)}
        }

        zipfilename = os.path.join(d, "test.zip")
        zft = ZipForTest(zipfilename, data)
        bucket = "test-dummy-bucket"
        q = multiprocessing.Queue()

        # Copy ZIP
        with from_url('s3://{}/'.format(bucket),
                      create_bucket=True) as s3:
            assert isinstance(s3, S3)
            with open(zipfilename, 'rb') as src,\
                    s3.open('test.zip', 'wb') as dst:
                shutil.copyfileobj(src, dst)

            with s3.open('test.zip', 'rb') as fp:
                assert zipfile.is_zipfile(fp)

        def child(zfs, worker_idx):
            try:
                for i in range(n_samples_per_worker):
                    sample_idx = (
                        worker_idx * n_samples_per_worker + i) // sample_size
                    filename = 'dir/file-{}'.format(sample_idx)
                    with zfs.open(filename, 'rb') as fp:
                        data1 = fp.read()
                    assert data['dir']['file-{}'.format(sample_idx)] == data1
                q.put(('ok', None))
            except Exception as e:
                traceback.print_tb()
                q.put(('ng', e))

        with from_url('s3://{}/test.zip'.format(bucket),
                      local_cache=True, reset_on_fork=True) as fs:

            # Add tons of data into the cache in parallel

            ps = [multiprocessing.Process(target=child, args=(fs, worker_idx))
                  for worker_idx in range(n_workers)]
            for p in ps:
                p.start()
            for p in ps:
                p.join()
                ok, e = q.get()
                assert 'ok' == ok, str(e)

            for worker_idx in range(n_workers):
                child(fs, worker_idx)
                ok, e = q.get()
                assert 'ok' == ok, str(e)


@mock_s3
def test_force_type2():
    with tempfile.TemporaryDirectory() as d:
        zipfilename = os.path.join(d, "test.zip")
        z = ZipForTest(zipfilename)
        bucket = "test-dummy-bucket"

        with from_url('s3://{}/'.format(bucket),
                      create_bucket=True) as s3:
            assert isinstance(s3, S3)
            with open(zipfilename, 'rb') as src,\
                    s3.open('test.zip', 'wb') as dst:
                shutil.copyfileobj(src, dst)

            with open(zipfilename, 'rb') as f1:
                f1data = f1.read()

            with s3.open('test.zip', 'rb') as f2:
                f2data = f2.read()

            assert f1data == f2data

        # It's not tested yet?!
        url = 's3://{}/test.zip'.format(bucket)
        with from_url(url, force_type='zip') as s3:
            with s3.open('file', 'rb') as fp:
                data = fp.read()
            assert z.content('file') == data

            with s3.open('dir/f', 'rb') as fp:
                data = fp.read()
            assert b'bar' == data

        with pytest.raises(ValueError):
            # from_url() is only for containers. In this case,
            # test.zip must be a directory or prefix, and thus it
            # should fail.
            from_url(url, force_type='file')
