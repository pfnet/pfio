from typing import Optional, Union, Callable, Any, List, Type
import io
from chainerio.profiler import profiling_decorator
from chainerio.profiler import Profiler


class FileObject(io.IOBase):
    def __init__(self,
                 base_file_object: Any,
                 base_filesystem_handler: Any,
                 io_profiler: Type[Profiler],
                 file_path: str, mode: str = "r", buffering: int = -1,
                 encoding: Optional[str] = None,
                 errors: Optional[str] = None,
                 newline: Optional[str] = None,
                 closefd: bool = True,
                 opener: Optional[Callable[[str, int], Any]] = None):

        self.base_file_object = base_file_object
        self.base_filesystem_handler = base_filesystem_handler
        self.io_profiler = io_profiler
        self.file_path = file_path
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd
        self.opener = opener

    @profiling_decorator
    def seek(self, pos: int, whence: int = 0) -> int:
        return self.base_file_object.seek(pos, whence)

    @profiling_decorator
    def close(self) -> None:
        return self.base_file_object.close()

    @profiling_decorator
    def flush(self) -> None:
        return self.base_file_object.flush()

    @profiling_decorator
    def tell(self) -> int:
        return self.base_file_object.tell()

    @profiling_decorator
    def truncate(self, size: Optional[int] = None) -> int:
        return self.base_file_object.truncate(size)

    @profiling_decorator
    def fileno(self) -> int:
        return self.base_file_object.fileno()

    @profiling_decorator
    def read(self, size: Optional[int] = None) -> Union[bytes, str]:
        return self.base_file_object.read(size)

    @profiling_decorator
    def read1(self, size: Optional[int] = None) -> bytes:
        return self.base_file_object.read1(size)

    @profiling_decorator
    def readinto(self, b: bytes) -> Optional[int]:
        return self.base_file_object.readinto(b)

    @profiling_decorator
    def write(self, b: Union[bytes, str]) -> None:
        return self.base_file_object.write(b)

    @profiling_decorator
    def readline(self, size: int = -1) -> Union[bytes, str]:
        return self.base_file_object.readline(size)

    @profiling_decorator
    def readlines(self, hint: Optional[int] = None) \
            -> Union[List[bytes], List[str]]:
        return self.base_file_object.readlines(hint)

    @profiling_decorator
    def writelines(self, lines: Union[List[str], List[bytes]]) -> None:
        return self.base_file_object.writelines(lines)
