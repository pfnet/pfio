# Test fs.FS compatibility
import contextlib
import multiprocessing as mp
import os
import tempfile

import pytest
from moto import mock_s3
from parameterized import parameterized

from pfio.testing import ZipForTest, randstring
from pfio.v2 import S3, Local, LocalFileStat, Zip, from_url, open_url
from pfio.v2.s3 import S3ObjectStat, S3PrefixStat


@contextlib.contextmanager
def gen_fs(target):
    if target == "s3":
        bucket = "test-dummy-bucket"
        with S3(bucket, create_bucket=True) as s3:
            yield s3
            # s3.client.delete_bucket(bucket)

    elif target == "local":
        with tempfile.TemporaryDirectory() as d:
            yield Local(d)

    else:
        raise RuntimeError()


@parameterized.expand(["s3", "local"])
@mock_s3
def test_smoke(target):
    filename = randstring()
    filename2 = randstring()
    content = randstring(1024) + '\n' + randstring(234)
    with gen_fs(target) as fs:
        with fs.open(filename, 'w') as fp:
            fp.write(content)

        with fs.open(filename, 'r') as fp:
            assert content == fp.read()

        with fs.open(filename, 'r') as fp:
            lines = fp.readlines()
            print(type(fp))
            assert 2 == len(lines)
            assert 1025 == len(lines[0])
            assert 234 == len(lines[1])

        assert filename in list(fs.list())

        fs.mkdir('d')

        with fs.open('d/foo', 'w') as fp:
            fp.write(content + content)

        with fs.open('d/foo', 'r') as fp:
            assert (content + content) == fp.read()

        print('recursive:', list(fs.list(recursive=True)))
        print('non-rec:', list(fs.list(recursive=False)))
        assert filename in list(fs.list())
        assert 2 == len(list(fs.list(recursive=False)))

        assert 2 == len(list(fs.list(recursive=False, detail=True)))
        for e in fs.list(recursive=False, detail=True):
            if target == "local":
                assert isinstance(e, LocalFileStat)
            elif target == "s3":
                if e.isdir():
                    assert isinstance(e, S3PrefixStat)
                else:
                    assert isinstance(e, S3ObjectStat)
            else:
                assert False

        assert 'd/' in list(fs.list(recursive=False))

        assert 'foo' in list(fs.list('d/'))

        st = fs.stat(filename)
        assert len(content) == st.size
        assert st.filename is not None
        assert st.last_modified is not None
        assert type(st.last_modified) == float

        with fs.open(filename2, 'wb') as fp:
            fp.write(content.encode())

        with fs.open(filename2, 'rb') as fp:
            buf2 = fp.read()

        assert content == buf2.decode()

        with fs.open(filename2, 'r') as fp:
            buf3 = fp.read()

        assert content == buf3

        fs.remove(filename)
        fs.remove(filename2)

        assert not fs.exists(filename)
        assert not fs.is_forked

        subfs = fs.subfs('d')
        assert subfs.exists('foo')


def test_from_url_force_type():
    with from_url(".", force_type='file') as fs:
        assert isinstance(fs, Local)

    with pytest.raises(ValueError):
        from_url(".", force_type='hdfs')

    with pytest.raises(ValueError):
        from_url(".", force_type='s3')

    with pytest.raises(ValueError):
        from_url(".", force_type='foobar')

    with tempfile.TemporaryDirectory() as d:
        zipfilename = os.path.join(d, "test.zip")
        ZipForTest(zipfilename)

        with from_url(zipfilename, force_type='zip') as fs:
            assert isinstance(fs, Zip)

        # Without forced type, try to open according to the suffix
        with from_url(zipfilename) as fs:
            assert isinstance(fs, Zip)

        with pytest.raises(ValueError):
            # In type 'file' is forced, target path should be a
            # directory regardless of the suffix
            from_url(zipfilename, force_type='file')

        testfilename = os.path.join(d, "test.txt")
        with open_url(testfilename, 'w') as fp:
            fp.write('hello')

        with open_url(testfilename, 'r', force_type='file') as fp:
            assert 'hello' == fp.read()

        with pytest.raises(ValueError):
            with open_url(testfilename, 'r', force_type='hdfs'):
                pass

        with pytest.raises(IsADirectoryError):
            with open_url(testfilename, 'r', force_type='zip'):
                pass


@parameterized.expand(["s3", "local"])
@mock_s3
def test_seekeable_read(target):
    filename = randstring()
    content = b'0123456789'
    with gen_fs(target) as fs:
        with fs.open(filename, 'wb') as fp:
            fp.write(content)

        print(content)
        for i, c in enumerate(content):
            with fs.open(filename, 'rb') as fp:
                fp.seek(i)
                s = fp.read()
                print(c, s)
                assert c == s[0]


class DummyLoader:
    def __init__(self, dirname, content):
        self.s3 = from_url(dirname)
        self.content = content

    def __call__(self):
        assert self.s3.is_forked

        # Should be reset and be able to read the contents
        with self.s3.open('file', 'rb') as fp:
            assert self.content == fp.read()


@mock_s3
def test_recreate():
    content = b'deadbeef'
    # TODO: test with hdfs?
    with gen_fs("s3") as fs:
        with fs.open('file', 'wb') as fp:
            fp.write(content)

    dirname = "s3://test-dummy-bucket/"

    # With forkserver set, it hangs
    # mp.set_start_method('forkserver', force=True)

    loader = DummyLoader(dirname, content)

    p = mp.Process(target=loader)
    p.start()
    p.join(timeout=1)
    assert p.exitcode == 0
