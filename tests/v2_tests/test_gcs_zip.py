import io
import json
import os
import shutil
import tempfile
import traceback
import zipfile

import pytest

import pfio
from pfio.testing import ZipForTest
from pfio.v2 import GoogleCloudStorage, Zip, from_url

# from moto import mock_aws, server

# BUCKET='my-pfio-test'
BUCKET = 'pfn-pfio-test-bucket'


def test_gcs_zip():
    with tempfile.TemporaryDirectory() as d:
        zipfilename = os.path.join(d, "test.zip")
        zft = ZipForTest(zipfilename)
        bucket = BUCKET

        with from_url(f'gs://{bucket}/',
                      create_bucket=True) as gcs:
            assert isinstance(gcs, GoogleCloudStorage)
            with open(zipfilename, 'rb') as src, \
                    gcs.open('test.zip', 'wb') as dst:
                shutil.copyfileobj(src, dst)

            with gcs.open('test.zip', 'rb') as fp:
                assert zipfile.is_zipfile(fp)

        with from_url(f'gs://{bucket}/test.zip') as z:
            assert isinstance(z, Zip)
            assert isinstance(z.fileobj, io.BufferedReader)

            assert zipfile.is_zipfile(z.fileobj)
            with z.open('file', 'rb') as fp:
                assert zft.content('file') == fp.read()

        with from_url(f'gs://{bucket}/test.zip',
                      buffering=0) as z:
            assert isinstance(z, Zip)
            assert 'buffering' in z.kwargs
            assert isinstance(z.fileobj, pfio.v2.gcs._ObjectReader)

            assert zipfile.is_zipfile(z.fileobj)
            with z.open('file', 'rb') as fp:
                assert zft.content('file') == fp.read()


@pytest.mark.parametrize("mp_start_method", ["fork", "forkserver"])
def test_gcs_zip_mp(mp_start_method):
    # mock_aws doesn't work well in forkserver, thus we use server-mode moto
    # address = "127.0.0.1"
    # port = 0  # auto-selection
    # moto_server = server.ThreadedMotoServer(
    #     ip_address=address,
    #     port=port
    # )
    # moto_server.start()
    # port = moto_server._server.port

    # kwargs = {
    #     "endpoint": f"http://{address}:{port}",
    #     "aws_access_key_id": "",
    #     "aws_secret_access_key": "",
    # }
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
        _ = ZipForTest(zipfilename, data)
        bucket = BUCKET

        # mp_ctx = multiprocessing.get_context(mp_start_method)
        # q = mp_ctx.Queue()

        # Copy ZIP
        # with from_url(f'gs://{bucket}/',
        #               create_bucket=True) as gcs:
        #     assert isinstance(gcs, GoogleCloudStorage)
        #     with open(zipfilename, 'rb') as src, \
        #             gcs.open('test.zip', 'wb') as dst:
        #         shutil.copyfileobj(src, dst)

        #     with gcs.open('test.zip', 'rb') as fp:
        #         assert zipfile.is_zipfile(fp)

        with from_url(f'gs://{bucket}/test.zip') as fs:
            # Add tons of data into the cache in parallel
            fs.multipart_upload('test.zip')

            # ps = [mp_ctx.Process(target=gcs_zip_mp_child,
            #                      args=(q, fs, worker_idx,
            #                            n_samples_per_worker, sample_size,
            #                            data)
            #                      )
            #       for worker_idx in range(n_workers)]
            # for p in ps:
            #     p.start()
            # for p in ps:
            #     p.join()
            #     ok, e = q.get()
            #     assert 'ok' == ok, str(e)

            # for worker_idx in range(n_workers):
            #     gcs_zip_mp_child(q, fs, worker_idx,
            #                     n_samples_per_worker, sample_size, data)
            #     ok, e = q.get()
            #     assert 'ok' == ok, str(e)

    # moto_server.stop()


def gcs_zip_mp_child(q, zfs, worker_idx,
                     n_samples_per_worker, sample_size, data):
    try:
        for i in range(n_samples_per_worker):
            sample_idx = (worker_idx * n_samples_per_worker + i) // sample_size
            filename = 'dir/file-{}'.format(sample_idx)
            with zfs.open(filename, 'rb') as fp:
                data1 = fp.read()
            assert data['dir']['file-{}'.format(sample_idx)] == data1
        q.put(('ok', None))
    except Exception as e:
        traceback.print_tb()
        q.put(('ng', e))


def test_force_type2():
    with tempfile.TemporaryDirectory() as d:
        zipfilename = os.path.join(d, "test.zip")
        z = ZipForTest(zipfilename)
        bucket = BUCKET

        with from_url(f'gs://{bucket}/',
                      create_bucket=True) as gcs:
            assert isinstance(gcs, GoogleCloudStorage)
            with open(zipfilename, 'rb') as src, \
                    gcs.open('test.zip', 'wb') as dst:
                shutil.copyfileobj(src, dst)

            with open(zipfilename, 'rb') as f1:
                f1data = f1.read()

            with gcs.open('test.zip', 'rb') as f2:
                f2data = f2.read()

            assert f1data == f2data

        # It's not tested yet?!
        url = f'gs://{bucket}/test.zip'
        with from_url(url, force_type='zip') as gcs:
            with gcs.open('file', 'rb') as fp:
                data = fp.read()
            assert z.content('file') == data

            with gcs.open('dir/f', 'rb') as fp:
                data = fp.read()
            assert b'bar' == data

        with pytest.raises(ValueError):
            # from_url() is only for containers. In this case,
            # test.zip must be a directory or prefix, and thus it
            # should fail.
            from_url(url, force_type='file')

        # Smoke test write mode
        url = f'gs://{bucket}/testw.zip'
        with from_url(url, force_type='zip', mode='w') as gcsz:
            k = "file"
            with gcsz.open(k, 'wb') as fp:
                fp.write(b"foo")

            k = "dir/f"
            with gcsz.open(k, 'wb') as fp:
                fp.write(b"bar")


def test_gcs_zip_profiling():
    ppe = pytest.importorskip("pytorch_pfn_extras")

    with tempfile.TemporaryDirectory() as tmpdir:
        zipfilename = os.path.join(tmpdir, "test.zip")
        zft = ZipForTest(zipfilename)
        bucket = BUCKET

        with from_url(f'gs://{bucket}/',
                      create_bucket=True) as gcs:
            with open(zipfilename, 'rb') as src, \
                    gcs.open('test.zip', 'wb') as dst:
                shutil.copyfileobj(src, dst)

        ppe.profiler.clear_tracer()
        with from_url(f'gs://{bucket}/test.zip',
                      trace=True) as fs:
            with fs.open('file', 'rb') as fp:
                assert zft.content('file') == fp.read()

        state = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(state['_event_list'])]

        assert "pfio.v2.Zip:create-zipfile-obj" in keys
        assert "pfio.v2.Zip:open" in keys
        assert "pfio.v2.Zip:read" in keys
        assert "pfio.v2.Zip:close" in keys

        assert "pfio.v2.S3:open" in keys
        assert "pfio.v2.S3:read" in keys
        assert "pfio.v2.S3:close" in keys

        assert "pfio.boto3:get_object" in keys
