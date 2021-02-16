import os

import chainer
from chainer.serializers import load_npz

import pfio


def _parse_filename(filename, separator='_'):
    tokens = filename.split(separator)
    return int(tokens[-1])


def _scan_directory(fs, directory):
    local_files = []
    try:
        local_files = fs.list(directory)
    except Exception as e:
        if chainer.is_debug():
            print("Cannot list directory {}: {}".format(directory, e))

    local_files = filter(lambda s: not s.startswith('tmp'), local_files)
    files = filter(None, [(_parse_filename(f), f) for f in local_files])
    files = list(files)
    files.sort()

    if len(files) > 0:
        _i, filename = max(files)
        return filename
    else:
        return None


def load_snapshot(target, directory, filename=None, fs=None,
                  fail_on_no_file=False):
    assert directory is not None or filename is not None
    if fs is None:
        fs = pfio
    elif isinstance(fs, str):
        fs = pfio.create_handler(fs)
    else:
        fs = fs

    if filename is None and directory is not None:
        filename = _scan_directory(fs, directory)

    if filename is None:
        if fail_on_no_file:
            raise RuntimeError('No snapshot found from %s' % directory)
        return

    if directory is not None:
        filename = os.path.join(directory, filename)

    with fs.open(filename, 'rb') as fp:
        load_npz(fp, target)
