# Test fs.FS compatibility
import io
import random
import string

from moto import mock_s3
from parameterized import parameterized

from pfio.v2 import S3, Local, fs, open_url


def randstring():
    letters = string.ascii_letters + string.digits
    return (''.join(random.choice(letters) for _ in range(16)))


def gen_fs(target):
    if target == "s3":
        bucket = "test-dummy-bucket"
        s3 = S3(bucket)
        s3.client.create_bucket(Bucket=bucket)
        return s3
    elif target == "local":
        return Local("/tmp")
    else:
        raise RuntimeError()


@parameterized.expand(["s3", "local"])
@mock_s3
def test_smoke(target):
    filename = randstring()
    content = randstring()
    with gen_fs(target) as fs:
        with fs.open(filename, 'w') as fp:
            fp.write(content)

        with fs.open(filename, 'r') as fp:
            assert content == fp.read()

        fs.remove(filename)


@mock_s3
def test_factory_open():
    assert isinstance(fs.from_url('.'), Local)
    with open_url('./setup.cfg') as fp:
        assert isinstance(fp, io.IOBase)

    assert isinstance(fs.from_url('s3://foobar/boom/bom'), S3)
    bucket = 'foobar'
    s3 = S3(bucket)
    s3.client.create_bucket(Bucket=bucket)
    with open_url('s3://foobar/boom/bom.txt', 'w') as fp:
        fp.write('hello')

    with open_url('s3://foobar/boom/bom.txt', 'r') as fp:
        assert 'hello' == fp.read()
