import os

from chainer.serializers import save_npz
from chainer.training.extensions.snapshot_writers import Writer

import pfio


class SimpleWriter(Writer):
    '''Ignores ``outdir`` specified Chainer snapshot

    And writes out to arbitrary path including any system supported by
    PFIO. Example usage::

      trainer = Trainer(updater, (10, 'epoch'))
      writer = pfio.chainer_extensions.SimpleWriter(
          'hdfs:///user/USER/tgan-experiment')
      trainer.extend(snapshot(writer=writer))

      trainer.run()


    TODO(kuenishi): If the backing system is HDFS or other network
    filesystem there might be the need for asynchronous
    writes. Regarding consistency, after queued for async write and
    before writes the DL model might start being updated by
    optimization. Thus taking snapshot should be done with a
    consistent memory copy passed to ``save_npz`` (or some other way
    to keep it consistent somehow.

    '''

    def __init__(self, directory: str, savefun=None, fs=None):
        assert directory is not None
        self.directory = directory
        self.savefun = save_npz if savefun is None else savefun
        if fs is None:
            self.fs = pfio
        elif isinstance(fs, str):
            self.fs = pfio.create_handler(fs)
        else:
            self.fs = fs

        if not self.fs.exists(self.directory):
            self.fs.makedirs(self.directory)

    def __call__(self, filename, _outdir, target):
        tmpname = 'tmp' + filename
        dest = os.path.join(self.directory, filename)
        tmpfile = os.path.join(self.directory, tmpname)
        make_backup = self.fs.exists(dest)
        if make_backup:
            bak = '{}.bak'.format(dest)
            self.fs.rename(dest, bak)
        with self.fs.open(tmpfile, 'wb') as fp:
            # HDFS does not support overwrite
            self.savefun(fp, target)
            self.fs.rename(tmpfile, dest)
        if make_backup:
            self.fs.remove(bak)
