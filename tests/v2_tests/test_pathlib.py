from moto import mock_s3

from pfio.v2 import S3, from_url, pathlib


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


@mock_s3
def test_s3_glob():
    bucket = "test-dummy-bucket"
    key = "it's me!deadbeef"
    secret = "asedf;lkjdf;a'lksjd"
    with S3(bucket, create_bucket=True):
        with from_url('s3://test-dummy-bucket/base',
                      aws_access_key_id=key,
                      aws_secret_access_key=secret) as s3:

            for i in range(10):
                p = pathlib.Path(str(i), fs=s3)
                assert '/base/{}'.format(i) == str(p.resolve())
                p.touch()
                assert p.exists()

            # glob test
            d = pathlib.Path(fs=s3)
            files = list(d.glob("*"))
            assert 10 == len(files)
            assert [str(i) for i in range(10)] == sorted(str(f) for f in files)

            d2 = pathlib.Path('/', fs=s3)
            files = list(d2.glob("*"))
            assert 10 == len(files)
            for f in files:
                assert f.startswith('base/')

            files = list(d2.glob("*/0"))
            assert ['base/0'] == files
            for f in files:
                assert f.startswith('base/')

            paths = ['foo', 'bar', 'baz/foo', 'baz/hoge/boom/huga']
            for p in paths:
                pathlib.Path(p, fs=s3).touch()

            d3 = pathlib.Path('baz', fs=s3)
            assert ['hoge/boom/huga'] == list(d3.glob('**/huga'))
