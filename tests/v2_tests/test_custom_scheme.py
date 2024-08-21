import os

from moto import mock_aws

import pfio


@mock_aws
def test_ini():
    try:
        prev = os.getenv('PFIO_CONFIG_PATH')
        os.environ['PFIO_CONFIG_PATH'] = './example.pfio.ini'

        pfio.v2.config._load_config()

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


@mock_aws
def test_add_custom_scheme():
    pfio.v2.config._load_config()

    pfio.v2.config.add_custom_scheme("foobar2", "file")
    pfio.v2.config.add_custom_scheme(
        "baz2",
        "s3",
        {
            "endpoint": "https://s3.example.com",
            "aws_access_key_id": "hoge",
            "aws_secret_access_key": os.environ["HOME"],
            "read_timeout": "120",
        },
    )

    assert {"scheme": "file"} == pfio.v2.config.get_custom_scheme("foobar2")
    assert {
        "scheme": "s3",
        "endpoint": "https://s3.example.com",
        "aws_access_key_id": "hoge",
        "aws_secret_access_key": os.environ["HOME"],
        "read_timeout": "120",
    } == pfio.v2.config.get_custom_scheme("baz2")

    with pfio.v2.from_url('foobar2://pfio/') as fs:
        assert isinstance(fs, pfio.v2.Local)

    with pfio.v2.from_url('baz2://pfio/', _skip_connect=True) as s3:
        assert isinstance(s3, pfio.v2.S3)

        assert 'https://s3.example.com' == s3.kwargs['endpoint_url']
        assert 'hoge' == s3.kwargs['aws_access_key_id']
        assert os.getenv('HOME') == s3.kwargs['aws_secret_access_key']
        assert 120 == s3.botocore_config['read_timeout']
