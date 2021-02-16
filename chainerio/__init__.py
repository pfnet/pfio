import sys

# make sure pfio is in sys.modules
import pfio  # NOQA

sys.modules[__name__] = __import__('pfio')
