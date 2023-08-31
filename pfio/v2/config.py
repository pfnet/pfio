import configparser
import os
from typing import Dict, Optional


def _default_config_file():
    path = os.getenv('PFIO_CONFIG_PATH')
    if path:
        return path

    basedir = os.getenv('XDG_CONFIG_HOME')
    if not basedir:
        basedir = os.path.join(os.getenv('HOME'), ".config")

    return os.path.join(basedir, "pfio.ini")


def _load_config():
    global _config
    config = configparser.ConfigParser()
    configfile = _default_config_file()
    config.read(configfile)
    _config = config


def add_custom_scheme(
    name: str,
    scheme: str,
    data: Optional[Dict[str, str]] = None,
) -> None:
    """Adds a custom scheme.

    Args:
        name (str): Name of the custom scheme.

        scheme (str): Name of the base scheme.

        data (dict, optional): Additional data required for the scheme.

    .. note:: This feature is experimental.
    """
    if _config is None:
        _load_config()
    if data is None:
        data = {}
    else:
        data = data.copy()

    data["scheme"] = scheme
    _config[name] = data


def get_custom_scheme(name: str) -> Optional[Dict[str, str]]:
    """Returns a custom scheme.

    Args:
        name (str): Name of the custom scheme.

    Returns:
        dict: Custom scheme data. ``None`` if the custom scheme is not
              registered.

    .. note:: This feature is experimental.
    """
    if _config is None:
        _load_config()
    if name not in _config:
        return None
    return dict(_config[name])


_config = None
