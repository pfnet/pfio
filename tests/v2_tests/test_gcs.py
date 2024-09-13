import io
import json
import multiprocessing as mp
import os
import pickle
import tempfile

import pytest

from pfio.v2 import GoogleCloudStorage, from_url, open_url
from pfio.v2.gcs import ObjectStat, PrefixStat, _ObjectReader

os.environ['CLOUDSDK_CORE_PROJECT'] = os.environ['PFIO_TEST_PROJECT_ID']
BUCKET_NAME = os.environ['PFIO_TEST_BUCKET_NAME']
BASE_URL = f'gs://{BUCKET_NAME}'
URL = BASE_URL + '/base'


@pytest.fixture
def gcs_fixture():
    # A test fixture which provides
    # - GCS mock. using this fixture is equivalent to using @mock_aws decorator
    # - Dummy credentials
    # - GCS filesystem with bucket creation
    class _GCSFixture:
        bucket_name = BUCKET_NAME
        fs = GoogleCloudStorage(bucket_name)

    yield _GCSFixture()


def touch(gcs, path, content):
    with gcs.open(path, 'w') as fp:
        fp.write(content)

    assert gcs.exists(path)


def test_gcs_init(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        assert gcs_fixture.bucket_name == gcs.bucket_name
        assert 'base' == gcs.cwd


def test_gcs_repr_str(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        repr(gcs)
        str(gcs)


def test_gcs_files(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        with gcs.open('foo.txt', 'w') as fp:
            fp.write('bar')
            assert not fp.closed

        assert ['foo.txt'] == list(gcs.list())
        assert [] == list(gcs.list('base'))
        assert [] == list(gcs.list('base/'))
        assert ['foo.txt'] == list(gcs.list('/base'))
        assert ['foo.txt'] == list(gcs.list('/base/'))

        assert ['foo.txt'] == list(gcs.list(recursive=True))
        assert ['base/', 'base/foo.txt'] == list(gcs.list('/', recursive=True))

        gcs.mkdir('subdir')
        e = list(gcs.list(detail=True))
        assert len(e) == 2   # ['foo.txt", "subdir/"]

        # "foo.txt"
        b = e[0]
        assert isinstance(b, ObjectStat)
        assert b.filename == 'foo.txt'
        assert b.size == 3
        assert b.metadata is None

        # "subdir/"
        f = e[1]
        assert isinstance(f, PrefixStat)
        assert f.filename == 'subdir/'
        assert f.size == -1

        gcs.remove('subdir', recursive=True)

        with gcs.open('dir/foo.txt', 'w') as fp:
            fp.write('bar')
            assert not fp.closed

        assert 'foo.txt' in list(gcs.list()) and 'dir/' in list(gcs.list())

        assert not gcs.isdir("foo.txt")
        assert gcs.isdir(".")
        assert gcs.isdir("/base/")
        assert gcs.isdir("/base")
        assert gcs.isdir("/")
        assert not gcs.isdir("/bas")

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


def test_gcs_init_with_timeouts(gcs_fixture):
    with from_url(BASE_URL + '/base',
                  connect_timeout=300) as gcs:
        assert isinstance(gcs, GoogleCloudStorage)
        assert (gcs.connect_time == 300)


# TODO: Find out a way to know buffer size used in a BufferedReader
@pytest.mark.parametrize("buffering, reader_type",
                         [(-1, io.BufferedReader),
                          (0, _ObjectReader),
                          (2, io.BufferedReader)])
def test_gcs_read(gcs_fixture, buffering, reader_type):
    with from_url(BASE_URL + '/base',
                  buffering=buffering) as gcs:
        with gcs.open('foo.txt', 'w') as fp:
            fp.write('bar')
            assert not fp.closed

        with gcs.open('foo.txt', 'r') as fp:
            assert isinstance(fp, io.TextIOWrapper)
            assert 'bar' == fp.read()
            assert not fp.closed

        with gcs.open('foo.txt', 'rb') as fp:
            assert isinstance(fp, reader_type)
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

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


def test_empty_file(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        # Create an empty file
        with gcs.open('foo.dat', 'wb'):
            pass

        # It should be able to read it without error
        with gcs.open('foo.dat', 'rb') as f:
            assert len(f.read()) == 0

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


def test_gcs_fork(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        with gcs.open('foo.txt', 'w') as fp:
            fp.write('bar')
            assert not fp.closed

        def f(gcs):
            with gcs.open('foo.txt', 'r') as fp:
                assert fp.read()

        p = mp.Process(target=f, args=(gcs,))
        p.start()
        p.join()
        assert p.exitcode == 0

        def g(gcs):
            with GoogleCloudStorage(bucket=BUCKET_NAME) as gcs2:
                with gcs2.open('base/foo.txt', 'r') as fp:
                    assert fp.read()

        p = mp.Process(target=g, args=(gcs,))
        p.start()
        p.join()
        assert p.exitcode == 0

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


def test_gcs_mpu(gcs_fixture):
    # Test multipart upload
    with GoogleCloudStorage(gcs_fixture.bucket_name,
                            create_bucket=True,
                            mpu_chunksize=8 * 1024 * 1024) as gcs:
        with gcs.open('testfile', 'wb') as fp:
            for _ in range(4):
                fp.write(b"01234567" * (1024 * 1024))

        with gcs.open('testfile', 'rb') as fp:
            data = fp.read()

        assert 8 * 1024 * 1024 * 4 == len(data)
        assert b"01234567" == data[:8]

        with gcs.open('testfile2', 'wb') as fp:
            for _ in range(4):
                fp.write(b"0123456" * (1024 * 1024))

        with gcs.open('testfile2', 'rb') as fp:
            data = fp.read()

        assert 7 * 1024 * 1024 * 4 == len(data)
        assert b"0123456" == data[7:14]

        with gcs.open('testfile2', 'w') as fp:
            for _ in range(4):
                fp.write("0123456" * (1024 * 1024))

        with gcs.open('testfile2', 'r') as fp:
            data = fp.read()

        assert 7 * 1024 * 1024 * 4 == len(data)
        assert "0123456" == data[7:14]

        # cleanup
        gcs.remove('testfile')
        gcs.remove('testfile2')


def test_gcs_recursive(gcs_fixture):
    # If the root folder has a folder other than base, this test will fail.
    with from_url(BASE_URL) as gcs:
        entries = list(gcs.list('/'))
        for e in entries:
            gcs.remove(e, recursive=True)

    with from_url(BASE_URL + '/base') as gcs:
        touch(gcs, 'foo.txt', 'bar')
        touch(gcs, 'bar.txt', 'baz')
        touch(gcs, 'baz/foo.txt', 'foo')

        expected = ['bar.txt', 'baz/', 'baz/foo.txt', 'foo.txt']
        assert expected == list(gcs.list(recursive=True))

        expected = [
            'base/',
            'base/bar.txt',
            'base/baz/',
            'base/baz/foo.txt',
            'base/foo.txt']
        abspaths = list(gcs.list('/', recursive=True))
        assert expected == list(gcs.list('/', recursive=True))

        for p in abspaths:
            assert p.startswith('base/')

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


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
def test_gcs_seek(gcs_fixture, buffering):
    with from_url(BASE_URL + '/base',
                  buffering=buffering) as gcs:
        # Make a 10-bytes test data
        touch(gcs, 'foo.data', '0123456789')

    with open_url(BASE_URL + '/base/foo.data', 'rb') as f:
        _seek_check(f)

    # Make sure the seek behavior is same as normal file-like objects.
    with tempfile.NamedTemporaryFile() as tmpf:
        # Make the same 10-bytes test data on local filesystem
        with open(tmpf.name, 'wb') as f:
            f.write(b'0123456789')

        # Open and check its seek behavior is identical
        with open(tmpf.name, 'rb') as f:
            _seek_check(f)

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


@pytest.mark.parametrize("buffering", [-1, 0])
def test_gcs_pickle(gcs_fixture, buffering):
    with from_url(BASE_URL + '/base', buffering=buffering) as gcs:
        with gcs.open('foo.pkl', 'wb') as fp:
            pickle.dump({'test': 'data'}, fp)

    with open_url(BASE_URL + '/base/foo.pkl', 'rb') as f:
        assert pickle.load(f) == {'test': 'data'}

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


@pytest.mark.parametrize("buffering", [-1, 0])
def test_rename(gcs_fixture, buffering):
    with from_url(BASE_URL + '/base',
                  buffering=buffering) as gcs:
        with gcs.open('foo.pkl', 'wb') as fp:
            pickle.dump({'test': 'data'}, fp)

        gcs.rename('foo.pkl', 'bar.pkl')

    with from_url(BASE_URL) as gcs:
        assert not gcs.exists('base/foo.pkl')
        assert gcs.exists('base/bar.pkl')

    with open_url(BASE_URL + '/base/bar.pkl', 'rb') as f:
        assert pickle.load(f) == {'test': 'data'}

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)


@pytest.mark.parametrize("buffering", [-1, 0])
def test_gcs_read_and_readall(gcs_fixture, buffering):
    with from_url(BASE_URL,
                  buffering=buffering) as gcs:
        # Make a 10-bytes test data
        touch(gcs, 'foo.data', '0123456789')

    with open_url(BASE_URL + '/foo.data', 'rb') as f:
        assert f.read() == b'0123456789'

        f.seek(5, os.SEEK_SET)
        assert f.read() == b'56789'

        f.seek(5, os.SEEK_SET)
        assert f.read(2) == b'56'

        f.seek(5, os.SEEK_SET)
        assert f.read(1000) == b'56789'

        f.seek(5, os.SEEK_SET)
        assert f.raw.readall() == b'56789'

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('foo.data')


@pytest.mark.parametrize("buffering", [-1, 0])
def test_gcs_readlines(gcs_fixture, buffering):
    with from_url(BASE_URL,
                  buffering=buffering) as gcs:
        # Make a 10-bytes test data
        txt = '''first line
second line
third line
'''
        touch(gcs, 'foo.txt', txt)

    with open_url(BASE_URL + '/foo.txt', 'r') as f:
        lines = f.readlines()

        assert "first line\n" == lines[0]
        assert "second line\n" == lines[1]
        assert "third line\n" == lines[2]

        # Test the undocumented feature still exists
        assert len(txt) == getattr(f, '_CHUNK_SIZE')
        f._CHUNK_SIZE = 233458
        assert 233458 == f._CHUNK_SIZE

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('foo.txt')


def test_mkdir(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        test_dir_name = "testmkdir"
        gcs.mkdir(test_dir_name)
        assert gcs.isdir(test_dir_name)

        # cleanup
        gcs.remove(test_dir_name, recursive=True)


def test_makedirs(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        test_dir_name = "testmkdir"
        nested_dir_name = test_dir_name + '/' + "nested_dir"
        gcs.makedirs(nested_dir_name)
        assert gcs.isdir(nested_dir_name)

        # cleanup
        gcs.remove(test_dir_name, recursive=True)


def test_exists(gcs_fixture):
    non_exist_file = "non_exist_file.txt"

    with from_url(BASE_URL + '/base') as gcs:
        assert gcs.exists('.')
        assert gcs.exists('/')
        assert not gcs.exists(non_exist_file)


def test_mkdir(gcs_fixture):
    with from_url(URL) as gcs:
        test_dir_name = "testmkdir"
        gcs.mkdir(test_dir_name)
        assert gcs.isdir(test_dir_name)

        # cleanup
        gcs.remove(test_dir_name, recursive=True)


def test_makedirs(gcs_fixture):
    with from_url(URL) as gcs:
        test_dir_name = "testmkdir"
        nested_dir_name = test_dir_name + '/' + "nested_dir"
        gcs.makedirs(nested_dir_name)
        assert gcs.isdir(nested_dir_name)

        # cleanup
        gcs.remove(test_dir_name, recursive=True)


def test_exists(gcs_fixture):
    non_exist_file = "non_exist_file.txt"

    with from_url(URL) as gcs:
        assert gcs.exists('.')
        assert gcs.exists('/')
        assert not gcs.exists(non_exist_file)


def test_remove(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        with pytest.raises(FileNotFoundError) as err:
            gcs.remove('non-existent-object')
        assert str(err.value) == "No such GCS object: 'non-existent-object'"

        touch(gcs, 'foo.data', '0123456789')
        assert gcs.exists('foo.data')
        gcs.remove('foo.data')
        assert not gcs.exists('foo.data')


def test_remove_folder(gcs_fixture):
    with from_url(BASE_URL + '/base') as gcs:
        d = 'subdir'
        gcs.mkdir(d)

        with pytest.raises(io.UnsupportedOperation) as err:
            gcs.remove(d)
        assert str(err.value) == \
            "Please add recursive=True to remove a directory"

        gcs.remove(d, recursive=True)
        assert not gcs.exists(d)


def test_fs_factory(gcs_fixture):
    with gcs_fixture.fs as gcs:
        with gcs.open('boom/baz.txt', 'w') as fp:
            fp.write('bom')

        with gcs.open('boom/baz.txt', 'r') as fp:
            assert 'bom' == fp.read()

    assert isinstance(from_url(BASE_URL), GoogleCloudStorage)
    assert isinstance(from_url(BASE_URL + '/boom'), GoogleCloudStorage)

    with open_url(BASE_URL + '/boom/bom.txt', 'w') as fp:
        fp.write('hello')

    with open_url(BASE_URL + '/boom/bom.txt', 'r') as fp:
        assert 'hello' == fp.read()

    with from_url(BASE_URL) as fs:
        assert isinstance(fs, GoogleCloudStorage)
        assert fs.exists('boom/bom.txt')
        with fs.open('boom/bom.txt', 'rt') as f:
            assert f.read() == 'hello'

    with from_url(BASE_URL + '/boom/') as fs:
        assert isinstance(fs, GoogleCloudStorage)
        assert fs.exists('bom.txt')
        with fs.open('bom.txt', 'rt') as f:
            assert f.read() == 'hello'

    # cleanup
    with gcs_fixture.fs as gcs:
        gcs.remove('boom', recursive=True)


def test_from_url_create_option(gcs_fixture):
    # In S3, actually create option has no effect
    path = f'gs://{gcs_fixture.bucket_name}/path/'
    with from_url(path) as fs:
        assert not fs.exists(path)

    with from_url(path, create=True) as fs:
        assert not fs.exists(path)


# TODO: Support Google Cloud Storage
def test_gcs_rw_profiling(gcs_fixture):
    ppe = pytest.importorskip("pytorch_pfn_extras")

    with from_url(BASE_URL + '/base', trace=True) as gcs:
        ppe.profiler.clear_tracer()

        with gcs.open('foo.txt', 'w') as fp:
            fp.write('bar')

        dict = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(dict['_event_list'])]

        assert "pfio.v2.S3:open" in keys
        assert "pfio.v2.S3:write" in keys
        assert "pfio.boto3:put_object" in keys
        assert "pfio.v2.S3:exit-context" in keys

    with from_url(BASE_URL + '/base', trace=True) as gcs:
        ppe.profiler.clear_tracer()

        with gcs.open('foo.txt', 'r') as fp:
            tmp = fp.read()
            assert tmp == 'bar'

        dict = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(dict['_event_list'])]

        assert "pfio.v2.S3:open" in keys
        assert "pfio.v2.S3:read" in keys
        assert "pfio.boto3:get_object" in keys
        assert "pfio.v2.S3:exit-context" in keys

    with from_url(BASE_URL + '/base', trace=True) as gcs:
        ppe.profiler.clear_tracer()

        fp = gcs.open('foo.txt', 'rb')
        tmp = fp.peek()
        assert tmp == b'bar'

        fp.close()

        dict = ppe.profiler.get_tracer().state_dict()
        keys = [event["name"] for event in json.loads(dict['_event_list'])]

        assert "pfio.v2.S3:open" in keys
        assert "pfio.v2.S3:peek" in keys
        assert "pfio.v2.S3:close" in keys

    # cleanup
    with from_url(BASE_URL) as gcs:
        gcs.remove('base', recursive=True)
