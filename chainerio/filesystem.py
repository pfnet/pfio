from chainerio.io import IO
from chainerio.profiler import IOProfiler

from chainerio._typing import Optional


class FileSystem(IO):

    def __init__(self, io_profiler: Optional[IOProfiler] = None,
                 root: str = ""):
        IO.__init__(self, io_profiler, root)
