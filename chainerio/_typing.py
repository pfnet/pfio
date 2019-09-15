import sys
import typing


g = globals()
for k in typing.__all__:
    g[k] = getattr(typing, k)

# workaround for old CPython bug
# https://github.com/chainer/chainerio/issues/28
if sys.version_info < (3, 5, 3):
    class DummyOptional(object):

        def __getitem__(self, *args, **kwargs):
            return typing.Any

    Optional = DummyOptional()

    class DummyUnion(object):

        def __getitem__(self, *args, **kwargs):
            return typing.Any

    Union = DummyUnion()
