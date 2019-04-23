import os
import pathlib
import shutil
import tempfile
import unittest

import chainerio

try:
    import chainer
    from chainer.training import extensions
    from chainer import testing
    chainer_available = True

    # They depend on Chainer
    from chainerio.chainer_extensions.snapshot_writers import SimpleWriter
    from chainerio.chainer_extensions import load_snapshot
except Exception:
    chainer_available = False


@unittest.skipIf(not chainer_available, "Chainer is not available")
def test_scan_directory():
    from chainerio.chainer_extensions.snapshot import _scan_directory
    with tempfile.TemporaryDirectory() as td:
        files = ['tmpfoobar_10', 'foobar_10', 'foobar_123', 'tmpfoobar_10234']
        for file in files:
            pathlib.Path(os.path.join(td, file)).touch()

        latest = _scan_directory(chainerio, td)
        assert latest is not None
        assert 'foobar_123' == latest


@unittest.skipIf(not chainer_available, "Chainer is not available")
def test_snapshot():
    trainer = testing.get_trainer_with_mock_updater()
    trainer.out = '.'
    trainer._done = True

    with tempfile.TemporaryDirectory() as td:
        writer = SimpleWriter(td)
        snapshot = extensions.snapshot(writer=writer)
        snapshot(trainer)
        assert 'snapshot_iter_0' in os.listdir(td)

        trainer2 = chainer.testing.get_trainer_with_mock_updater()
        load_snapshot(trainer2, td, fail_on_no_file=True)


@unittest.skipIf(shutil.which('hdfs') is None, "HDFS client not installed")
@unittest.skipIf(not chainer_available, "Chainer is not available")
def test_snapshot_hdfs():
    trainer = chainer.testing.get_trainer_with_mock_updater()
    trainer.out = '.'
    trainer._done = True

    with chainerio.create_handler('hdfs') as fs:
        tmpdir = "some-chainerio-tmp-dir"
        fs.makedirs(tmpdir, exist_ok=True)
        file_list = list(fs.list(tmpdir))
        assert len(file_list) == 0

        writer = SimpleWriter(tmpdir, fs=fs)
        snapshot = extensions.snapshot(writer=writer)
        snapshot(trainer)

        assert 'snapshot_iter_0' in fs.list(tmpdir)

        trainer2 = chainer.testing.get_trainer_with_mock_updater()
        load_snapshot(trainer2, tmpdir, fs=fs, fail_on_no_file=True)

        # Cleanup
        fs.remove(tmpdir, recursive=True)
