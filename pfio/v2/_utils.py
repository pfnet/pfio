from typing import Any, Dict, Type


def format_repr(cls: Type, data: Dict[str, Any]) -> str:
    data_str = ", ".join(f"{name}={value!r}" for name, value in data.items())
    return f"{cls.__module__}.{cls.__name__}({data_str})"
