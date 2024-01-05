import contextlib
import tempfile

import pytest
from moto import mock_s3

from pfio.v2 import S3, Local, from_url, pathlib


def test_name():
    p = pathlib.Path('foo.txt')
    assert 'foo.txt' == p.name

    p = pathlib.Path('foo/')
    assert 'foo' == p.name


def test_suffix():
    p = pathlib.Path('foo.txt')
    assert '.txt' == p.suffix

    assert 'foo.asc' == str(p.with_suffix('.asc'))
    assert '.txt' == p.suffix


def test_parent():
    p = pathlib.Path('foo/foo.txt')
    assert 'foo' == str(p.parent)

    p = pathlib.Path('/')
    assert '/' == str(p.parent)

    # Not sure why Python's official pathlib works as Path().parent
    # refers to Path('.') {fliptable}
    # p = pathlib.Path()
    # assert '.' == str(p.parent)


def test_resolve():
    p = pathlib.Path('/')
    assert '/' == str(p.resolve())


def test_parts():
    assert pathlib.Path().parts == ()
    assert pathlib.Path('').parts == ()
    assert pathlib.Path('.').parts == ()
    assert pathlib.Path('/').parts == ('/',)
    assert pathlib.Path('./').parts == ()
    assert pathlib.Path('.//.').parts == ()
    assert pathlib.Path('a').parts == ('a',)
    assert pathlib.Path('/a').parts == ('/', 'a')
    assert pathlib.Path('/a', 'bb/cc', 'dd').parts == (
        '/', 'a', 'bb', 'cc', 'dd')
    assert pathlib.Path('a', '/b', 'c').parts == ('/', 'b', 'c')


def test_root():
    assert pathlib.Path().root == ''
    assert pathlib.Path('').root == ''
    assert pathlib.Path('.').root == ''
    assert pathlib.Path('/').root == '/'
    assert pathlib.Path('./').root == ''
    assert pathlib.Path('.//.').root == ''
    assert pathlib.Path('a').root == ''
    assert pathlib.Path('/a').root == '/'
    assert pathlib.Path('/a', 'bb/cc', 'dd').root == '/'
    assert pathlib.Path('a', '/b', 'c').root == '/'


@mock_s3
def test_s3():
    bucket = "test-dummy-bucket"
    key = "it's me!deadbeef"
    secret = "asedf;lkjdf;a'lksjd"
    with S3(bucket, create_bucket=True):
        with from_url('s3://test-dummy-bucket/base',
                      aws_access_key_id=key,
                      aws_secret_access_key=secret) as s3:

            p = pathlib.Path('foo', 'bar', 'baz', fs=s3)
            assert '/base/foo/bar/baz' == str(p.resolve())
            assert isinstance(p.resolve(), pathlib.Path)

            assert not p.is_dir()
            assert not p.is_absolute()
            assert not p.exists()
            p.touch()
            assert p.exists()
            assert '' == p.open().read()

            p = pathlib.Path('/', 'foo', 'bar', 'baz', fs=s3)
            assert '/foo/bar/baz' == str(p.resolve())
            assert p.is_absolute()

            p1 = pathlib.Path('/base', 'foo', 'bar', 'baz', fs=s3)
            p2 = pathlib.Path('foo', 'bar', 'baz', fs=s3)
            assert p1.samefile(p2)

            # Path / operator
            p3 = pathlib.Path('foo')
            p4 = p1 / p3
            assert '/base/foo/bar/baz/foo' == str(p4.resolve())

            p5 = p2 / 'foo'
            assert isinstance(p5, pathlib.Path)
            p6 = 'foo2' / p2
            p7 = p2 / 'bar' / 'fab'

            assert '/base/foo/bar/baz/foo' == str(p5.resolve())
            assert '/base/foo2/foo/bar/baz' == str(p6.resolve())
            assert '/base/foo/bar/baz/bar/fab' == str(p7.resolve())

            # Paths in S3 cannot be any directory
            assert not p5.is_dir()


def _setup_fs_fixture(fs):
    # /base/0 ... /base/9
    for i in range(3):
        p = pathlib.Path(str(i), fs=fs)
        p.touch()
        assert p.exists()
    # /0
    pathlib.Path("dir", fs=fs).mkdir()
    pathlib.Path("dir/0", fs=fs).touch()


@contextlib.contextmanager
def s3_fs():
    bucket = "test-dummy-bucket"
    key = "it's me!deadbeef"
    secret = "asedf;lkjdf;a'lksjd"
    with S3(bucket, create_bucket=True):
        with from_url('s3://test-dummy-bucket/base',
                      aws_access_key_id=key,
                      aws_secret_access_key=secret) as s3:
            _setup_fs_fixture(s3)
            yield s3


@contextlib.contextmanager
def local_fs():
    with tempfile.TemporaryDirectory() as tempd:
        with from_url(tempd) as local:
            _setup_fs_fixture(local)
            yield local


parameterize_fs = pytest.mark.parametrize(
    'fs_fixture',
    [s3_fs, local_fs]
)


@parameterize_fs
@mock_s3
def test_s3_iterdir(fs_fixture):
    with fs_fixture() as fs:
        d = pathlib.Path(fs=fs)
        files = list(d.iterdir())
        assert 4 == len(files)
        assert all([isinstance(f, pathlib.Path) for f in files])
        assert all([f._fs is fs for f in files])
        assert sorted(str(f.name) for f in files) == ["0", "1", "2", "dir"]


@parameterize_fs
@mock_s3
def test_s3_glob1(fs_fixture):
    with fs_fixture() as fs:
        d = pathlib.Path(fs=fs)
        files = list(d.glob("*"))
        assert 4 == len(files)
        assert all([isinstance(f, pathlib.Path) for f in files])
        assert all([f._fs is fs for f in files])
        assert sorted(str(f.name) for f in files) == ["0", "1", "2", "dir"]


@parameterize_fs
@mock_s3
def test_s3_glob2(fs_fixture):
    with fs_fixture() as fs:
        if isinstance(fs, Local):
            pytest.skip("Can't test absolute path for local FS")

        d2 = pathlib.Path('/', fs=fs)
        files = list(d2.glob("*"))
        assert 1 == len(files)
        assert all([isinstance(f, pathlib.Path) for f in files])
        assert all([f._fs is fs for f in files])
        assert sorted(str(f) for f in files) == ["/base"]


@parameterize_fs
@mock_s3
def test_s3_glob3(fs_fixture):
    with fs_fixture() as fs:
        d2 = pathlib.Path(fs=fs)
        files = list(d2.glob("*/0"))
        assert 1 == len(files)
        assert all([isinstance(f, pathlib.Path) for f in files])
        assert all([f._fs is fs for f in files])
        print(">", files)
        assert sorted(str(f) for f in files) == ["dir/0"]


@parameterize_fs
@mock_s3
def test_s3_glob4(fs_fixture):
    with fs_fixture() as fs:
        paths = ['foo', 'bar', 'baz/foo', 'baz/hoge/boom/huga']
        for p in paths:
            pathlib.Path(p, fs=fs).parent.mkdir(parents=True, exist_ok=True)
            pathlib.Path(p, fs=fs).touch()

        d3 = pathlib.Path('baz', fs=fs)
        files = list(d3.glob('**/huga'))
        assert 1 == len(files)
        assert all([isinstance(f, pathlib.Path) for f in files])
        assert all([f._fs is fs for f in files])
        assert ['baz/hoge/boom/huga'] == [str(f) for f in files]


@parameterize_fs
@mock_s3
def test_scope(fs_fixture):
    with fs_fixture() as fs:

        p1 = pathlib.Path("2")
        # Under no context, it tries to do Local().open("2")
        assert "2" == str(p1)
        with pytest.raises(FileNotFoundError):
            p1.open().read()

        with fs.scope():
            assert "docs" == str(pathlib.Path("docs"))

            # Under a known context it reads the data from fixture
            assert '' == p1.open().read()

            p2 = pathlib.Path("2")
            assert '' == p2.open().read()
