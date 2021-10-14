import os
import shutil
import tempfile

import pytest
from moto import mock_s3

from pfio.testing import ZipForTest
from pfio.v2 import S3, from_url


@mock_s3
def test_s3_zip():
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
