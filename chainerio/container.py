from chainerio.io import IO
from chainerio.profiler import Profiler

from typing import Optional, Type


class Container(IO):

    def __init__(self, base_handler: IO, base: str,
                 io_profiler: Optional[Type[Profiler]] = None, root: str = ""):
        IO.__init__(self, io_profiler, root)
        self.base_handler = base_handler
        self.base = base

    def reset_base_handler(self, handler: IO) -> None:
        self.base_handler = handler

    def rename(self, src: str, dst: str) -> None:
        raise NotImplementedError()
