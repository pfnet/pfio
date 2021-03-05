import warnings

from pfio.chainer_extensions.snapshot import load_snapshot  # NOQA

warnings.warn("Chainer extentions are deprecated and "
              "will be removed. Please use 'pfio' instead.",
              DeprecationWarning)
