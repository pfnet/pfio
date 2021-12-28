import io
import multiprocessing as mp
import os
import pickle
import tempfile

import pytest
from moto import mock_s3

from pfio.v2 import S3, from_url, open_url
from pfio.v2.fs import ForkedError
from pfio.v2.s3 import _ObjectReader


@pytest.fixture
def s3_fixture():
    # A test fixture which provides
    # - S3 mock. using this fixture is equivalent to using @mock_s3 decorator
    # - Dummy credentials
    # - S3 filesystem with bucket creation
    with mock_s3():
        class _S3Fixture():
            bucket = "test-bucket"
            aws_kwargs = {
                "aws_access_key_id": "it's me!deadbeef",
                "aws_secret_access_key": "asedf;lkjdf;a'lksjd",
            }
            fs = S3(bucket, create_bucket=True)

        yield _S3Fixture()


@pytest.mark.parametrize("buffering", [-1, 0])
def test_s3(s3_fixture, buffering):
    with from_url('s3://test-bucket/base',
                  buffering=buffering,
                  **s3_fixture.aws_kwargs) as s3:
        assert s3_fixture.bucket == s3.bucket
        assert '/base' == s3.cwd
        assert s3_fixture.aws_kwargs['aws_access_key_id'] \
            == s3.aws_access_key_id
        assert s3_fixture.aws_kwargs['aws_secret_access_key'] \
            == s3.aws_secret_access_key
        assert s3.endpoint is None

        with s3.open('foo.txt', 'w') as fp:
            fp.write('bar')
            assert not fp.closed

        with s3.open('foo.txt', 'r') as fp:
            assert 'bar' == fp.read()
            assert not fp.closed

        with s3.open('foo.txt', 'rb') as fp:
            assert b'b' == fp.read(1)
            assert b'a' == fp.read(1)
            assert b'r' == fp.read(1)
            assert b'' == fp.read(1)
            assert b'' == fp.read(1)
            fp.seek(1)
            assert b'a' == fp.read(1)
            assert b'r' == fp.read(1)
            assert b'' == fp.read(1)
            assert not fp.closed

        assert ['foo.txt'] == list(s3.list())
        assert [] == list(s3.list('base'))
        assert [] == list(s3.list('base/'))
        assert ['foo.txt'] == list(s3.list('/base'))
        assert ['foo.txt'] == list(s3.list('/base/'))

        assert ['foo.txt'] == list(s3.list(recursive=True))
        assert ['base/foo.txt'] == list(s3.list('/', recursive=True))

        with s3.open('dir/foo.txt', 'w') as fp:
            fp.write('bar')
            assert not fp.closed

        assert ['dir/', 'foo.txt'] == list(s3.list())

        assert not s3.isdir("foo.txt")
        assert s3.isdir(".")
        assert s3.isdir("/base/")
        assert s3.isdir("/base")
        assert not s3.isdir("/bas")

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

        def g(s3):
            try:
                with S3(bucket='test-bucket', **s3_fixture.aws_kwargs) as s4:
                    with s4.open('base/foo.txt', 'r') as fp:
                        fp.read()
            except ForkedError:
                pytest.fail('ForkedError')

        p = mp.Process(target=g, args=(s3,))
        p.start()
        p.join()
        assert p.exitcode == 0


def test_s3_mpu(s3_fixture):
    # Test multipart upload
    with S3(s3_fixture.bucket, create_bucket=True, mpu_chunksize=8*1024*1024,
            **s3_fixture.aws_kwargs) as s3:
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

        with s3.open('testfile2', 'w') as fp:
            for _ in range(4):
                fp.write("0123456" * (1024*1024))

        with s3.open('testfile2', 'r') as fp:
            data = fp.read()

        assert 7 * 1024 * 1024 * 4 == len(data)
        assert "0123456" == data[7:14]


def touch(s3, path, content):
    with s3.open(path, 'w') as fp:
        fp.write(content)

    assert s3.exists(path)


def test_s3_recursive(s3_fixture):
    with from_url('s3://test-bucket/base', **s3_fixture.aws_kwargs) as s3:

        touch(s3, 'foo.txt', 'bar')
        touch(s3, 'bar.txt', 'baz')
        touch(s3, 'baz/foo.txt', 'foo')

        assert 3 == len(list(s3.list(recursive=True)))
        abspaths = list(s3.list('/', recursive=True))
        assert 3 == len(abspaths)
        for p in abspaths:
            assert p.startswith('base/')


def test_buffering_default(s3_fixture):
    with from_url('s3://test-bucket/', **s3_fixture.aws_kwargs) as s3:

        touch(s3, 'foo.data', '0123456789')

    # TODO: Find out a way to check the buffer size from BufferSize
    with open_url('s3://test-bucket/foo.data', 'rb',
                  **s3_fixture.aws_kwargs) as f:
        assert isinstance(f, io.BufferedReader)
        assert f.read() == b'0123456789'

    with from_url('s3://test-bucket') as fs:
        f = fs.open('foo.data', 'rb')
        assert isinstance(f, io.BufferedReader)
        assert f.read() == b'0123456789'


def test_buffering_custom(s3_fixture):
    with from_url('s3://test-bucket/', **s3_fixture.aws_kwargs) as s3:

        touch(s3, 'foo.data', '0123456789')

    # TODO: Find out a way to check the buffer size from BufferSize
    with open_url('s3://test-bucket/foo.data', 'rb',
                  **s3_fixture.aws_kwargs,
                  buffering=5) as f:
        assert isinstance(f, io.BufferedReader)
        assert f.read() == b'0123456789'

    with from_url('s3://test-bucket', buffering=5) as fs:
        f = fs.open('foo.data', 'rb')
        assert isinstance(f, io.BufferedReader)
        assert f.read() == b'0123456789'


def test_buffering_no_buffer(s3_fixture):
    with from_url('s3://test-bucket/', **s3_fixture.aws_kwargs) as s3:

        touch(s3, 'foo.data', '0123456789')

    with open_url('s3://test-bucket/foo.data', 'rb',
                  **s3_fixture.aws_kwargs,
                  buffering=0) as f:
        assert isinstance(f, _ObjectReader)
        assert f.read() == b'0123456789'

    with from_url('s3://test-bucket', buffering=0) as fs:
        f = fs.open('foo.data', 'rb')
        assert isinstance(f, _ObjectReader)
        assert f.read() == b'0123456789'


def _seek_check(f):
    # Seek by absolute position
    ###########################
    assert f.seek(0, os.SEEK_SET) == 0 and f.read() == b'0123456789'
    assert f.seek(5, os.SEEK_SET) == 5 and f.read() == b'56789'
    assert f.seek(15, os.SEEK_SET) == 15 and f.read() == b''

    with pytest.raises(OSError) as err:
        f.seek(-1, os.SEEK_SET)
    assert err.value.errno == 22
    assert f.tell() == 15, "the position should be kept after an error"

    # Relative seek
    ###############
    f.seek(0, os.SEEK_SET)  # back to the start
    assert f.seek(5, os.SEEK_CUR) == 5
    assert f.seek(3, os.SEEK_CUR) == 8
    assert f.seek(4, os.SEEK_CUR) == 12
    assert f.seek(-1, os.SEEK_CUR) == 11

    f.seek(0, os.SEEK_SET)
    with pytest.raises(OSError) as err:
        f.seek(-1, os.SEEK_CUR)
    assert err.value.errno == 22
    assert f.tell() == 0, "the position should be kept after an error"

    # Seek from the tail
    ####################
    assert f.seek(0, os.SEEK_END) == 10
    assert f.seek(-2, os.SEEK_END) == 8
    assert f.seek(2, os.SEEK_END) == 12

    with pytest.raises(OSError) as err:
        f.seek(-11, os.SEEK_END) == 0
    assert err.value.errno == 22
    assert f.tell() == 12, "the position should be kept after an error"


@pytest.mark.parametrize("buffering", [-1, 0])
def test_s3_seek(s3_fixture, buffering):
    with from_url('s3://test-bucket/base',
                  buffering=buffering,
                  **s3_fixture.aws_kwargs) as s3:

        # Make a 10-bytes test data
        touch(s3, 'foo.data', '0123456789')

    with open_url('s3://test-bucket/base/foo.data', 'rb',
                  **s3_fixture.aws_kwargs) as f:
        _seek_check(f)

    # Make sure the seek behavior is same as normal file-like objects.
    with tempfile.NamedTemporaryFile() as tmpf:
        # Make the same 10-bytes test data on local filesystem
        with open(tmpf.name, 'wb') as f:
            f.write(b'0123456789')

        # Open and check its seek behavior is identical
        with open(tmpf.name, 'rb') as f:
            _seek_check(f)


@pytest.mark.parametrize("buffering", [-1, 0])
def test_s3_pickle(s3_fixture, buffering):
    with from_url('s3://test-bucket/base',
                  buffering=buffering,
                  **s3_fixture.aws_kwargs) as s3:

        with s3.open('foo.pkl', 'wb') as fp:
            pickle.dump({'test': 'data'}, fp)

    with open_url('s3://test-bucket/base/foo.pkl', 'rb',
                  **s3_fixture.aws_kwargs) as f:
        assert pickle.load(f) == {'test': 'data'}


@pytest.mark.parametrize("buffering", [-1, 0])
def test_rename(s3_fixture, buffering):
    with from_url('s3://test-bucket/base',
                  buffering=buffering,
                  **s3_fixture.aws_kwargs) as s3:

        with s3.open('foo.pkl', 'wb') as fp:
            pickle.dump({'test': 'data'}, fp)

        s3.rename('foo.pkl', 'bar.pkl')

    with from_url('s3://test-bucket', **s3_fixture.aws_kwargs) as s3:
        assert not s3.exists('base/foo.pkl')
        assert s3.exists('base/bar.pkl')

    with open_url('s3://test-bucket/base/bar.pkl', 'rb',
                  **s3_fixture.aws_kwargs) as f:
        assert pickle.load(f) == {'test': 'data'}


@pytest.mark.parametrize("buffering", [-1, 0])
def test_s3_read_and_readall(s3_fixture, buffering):
    with from_url('s3://test-bucket/',
                  buffering=buffering,
                  **s3_fixture.aws_kwargs) as s3:

        # Make a 10-bytes test data
        touch(s3, 'foo.data', '0123456789')

    with open_url('s3://test-bucket/foo.data', 'rb',
                  **s3_fixture.aws_kwargs) as f:
        assert f.read() == b'0123456789'

        f.seek(5, os.SEEK_SET)
        assert f.read() == b'56789'

        f.seek(5, os.SEEK_SET)
        assert f.read(2) == b'56'

        f.seek(5, os.SEEK_SET)
        assert f.read(1000) == b'56789'

        f.seek(5, os.SEEK_SET)
        assert f.raw.readall() == b'56789'


def test_remove(s3_fixture):
    with from_url('s3://test-bucket/base', **s3_fixture.aws_kwargs) as s3:
        with pytest.raises(FileNotFoundError) as err:
            s3.remove('non-existent-object')
        assert str(err.value) == "No such S3 object: 'non-existent-object'"

        touch(s3, 'foo.data', '0123456789')
        assert s3.exists('foo.data')
        s3.remove('foo.data')
        assert not s3.exists('foo.data')
