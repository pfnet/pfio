import multiprocessing as mp

import pytest
from moto import mock_s3

from pfio.v2 import S3, from_url
from pfio.v2.fs import ForkedError


@mock_s3
def test_s3():
    bucket = "test-dummy-bucket"
    key = "it's me!deadbeef"
    secret = "asedf;lkjdf;a'lksjd"
    with S3(bucket, create_bucket=True):
        with from_url('s3://test-dummy-bucket/base',
                      aws_access_key_id=key,
                      aws_secret_access_key=secret) as s3:
            assert bucket == s3.bucket
            assert '/base' == s3.cwd
            assert key == s3.aws_access_key_id
            assert secret == s3.aws_secret_access_key
            assert s3.endpoint is None

            with s3.open('foo.txt', 'w') as fp:
                fp.write('bar')
                assert not fp.closed

            with s3.open('foo.txt', 'r') as fp:
                assert 'bar' == fp.read()
                assert not fp.closed

            assert ['foo.txt'] == list(s3.list())
            assert [] == list(s3.list('base'))
            assert [] == list(s3.list('base/'))
            assert ['foo.txt'] == list(s3.list('/base'))
            assert ['foo.txt'] == list(s3.list('/base/'))

            def f(s3):
                try:
                    s3.open('foo.txt', 'r')
                except ForkedError:
                    pass
                else:
                    pytest.fail('No Error on Forking')

            p = mp.Process(target=f, args=(s3,))
            p.start()
            p.join()
            assert p.exitcode == 0

    # Test multipart upload
    bucket = 'test-mpu'
    with S3(bucket, create_bucket=True, mpu_chunksize=8*1024*1024,
            aws_access_key_id=key,
            aws_secret_access_key=secret) as s3:
        with s3.open('testfile', 'wb') as fp:
            for _ in range(4):
                fp.write(b"01234567" * (1024*1024))

        with s3.open('testfile', 'rb') as fp:
            data = fp.read()

        assert 8 * 1024 * 1024 * 4 == len(data)
        assert b"01234567" == data[:8]

        with s3.open('testfile2', 'wb') as fp:
            for _ in range(4):
                fp.write(b"0123456" * (1024*1024))

        with s3.open('testfile2', 'rb') as fp:
            data = fp.read()

        assert 7 * 1024 * 1024 * 4 == len(data)
        assert b"0123456" == data[7:14]
