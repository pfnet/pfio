import sys
import typing

# workaround for old CPython bug
# https://github.com/pfnet/pfio/issues/28
if sys.version_info < (3, 5, 3):
    class DummyOptional(object):

        def __getitem__(self, *args, **kwargs):
            return typing.Any

    Optional = DummyOptional()

    class DummyUnion(object):

        def __getitem__(self, *args, **kwargs):
            return typing.Any

    Union = DummyUnion()
else:
    Optional = typing.Optional
    Union = typing.Union
