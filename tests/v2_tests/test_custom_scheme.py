import os

from moto import mock_s3

import pfio


@mock_s3
def test_smoke():
    try:
        prev = os.getenv('PFIO_CONFIG_PATH')
        os.environ['PFIO_CONFIG_PATH'] = './example.pfio.ini'

        with pfio.v2.from_url('foobar://pfio/') as fs:
            assert isinstance(fs, pfio.v2.Local)

        with pfio.v2.from_url('baz://pfio/', _skip_connect=True) as s3:
            assert isinstance(s3, pfio.v2.S3)

            assert 'https://s3.example.com' == s3.kwargs['endpoint_url']
            assert 'hoge' == s3.kwargs['aws_access_key_id']
            assert os.getenv('HOME') == s3.kwargs['aws_secret_access_key']

    finally:
        if prev:
            os.environ['PFIO_CONFIG_PATH'] = prev
        else:
            del os.environ['PFIO_CONFIG_PATH']
            assert not os.getenv('PFIO_CONFIG_PATH')
