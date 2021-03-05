import sys
import warnings

warnings.warn("package 'chainerio' is deprecated and will be removed."
              " Please use 'pfio' instead.",
              DeprecationWarning)

# make sure pfio is in sys.modules
import pfio  # NOQA

sys.modules[__name__] = __import__('pfio')
