from typing import Optional, Union, Callable, Any, List
import io


class FileObject(io.IOBase):
    def __init__(self,
                 base_file_object: Any,
                 base_filesystem_handler: Any,
                 io_profiler, path: str, mode: str = "r", buffering: int = -1,
                 encoding: Optional[str] = None,
                 errors: Optional[str] = None,
                 newline: Optional[str] = None,
                 closefd: bool = True,
                 opener: Optional[Callable[[str, int], Any]] = None):

        self.base_file_object = base_file_object
        self.base_filesystem_handler = base_filesystem_handler
        self.io_profiler = io_profiler
        self.path = path
        self.mode = mode
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd
        self.opener = opener

    def seek(self, pos: int, whence: int = 0) -> int:
        return self.base_file_object.seek(pos, whence)

    def close(self) -> None:
        return self.base_file_object.close()

    def flush(self) -> None:
        return self.base_file_object.flush()

    def tell(self) -> int:
        return self.base_file_object.tell()

    def truncate(self, size: Optional[int] = None) -> int:
        return self.base_file_object.truncate(size)

    def fileno(self) -> int:
        return self.base_file_object.fileno()

    def read(self, size: Optional[int] = None) -> Union[bytes, str]:
        return self.base_file_object.read(size)

    def read1(self, size: Optional[int] = None) -> bytes:
        return self.base_file_object.read1(size)

    def readinto(self, b: bytes) -> Optional[int]:
        return self.base_file_object.readinto(b)

    def write(self, b: Union[bytes, str]) -> None:
        return self.base_file_object.write(b)

    def readline(self, size: int = -1) -> Union[bytes, str]:
        return self.base_file_object.readline(size)

    def readlines(self, hint: Optional[int] = None) \
            -> Union[List[bytes], List[str]]:
        return self.base_file_object.readlines(hint)

    def writelines(self, lines: Union[List[str], List[bytes]]) -> None:
        return self.base_file_object.writelines(lines)
