## PFIO

PFIO is an IO abstraction library developed by PFN, optimized for deep
learning training with batteries included. It supports

- Filesystem API abstraction with unified error semantics,
- Explicit user-land caching system,
- IO performance tracing and metrics stats, and
- Fileset container utilities to save metadata.


## Dependency

- HDFS client and libhdfs for HDFS access
- CPython >= 3.6

## Installation and Document build

Installation

```shell
$ git clone https://github.com/pfnet/pfio.git
$ cd pfio
$ pip install .
```

Documentation
```sh
$ cd pfio/docs
$ make html
$ open build/html/index.html
```

Test
```sh
$ cd pfio
$ pip install .[test]
$ pytest tests/
```

## How to use

Please refer to the [official document](https://pfio.readthedocs.io) for more information about the usage.

## Release

Check [the official document](https://packaging.python.org/tutorials/packaging-projects/) for latest release procedure.

Run tests locally:

```sh
$ pip install --user -e .[test]
$ pytest
```

Bump version numbers in `pfio/version.py` .

Push and open a pull request to invoke CI. Once CI passed and the pull request merged,
tag a release:

```sh
$ git tag -s X.Y.Z
$ git push --tags
```

Build:

```sh
$ rm -rf dist
$ pip3 install --user build
$ python3 -m build
```

Release to PyPI:

```sh
$ python3 -m pip install --user --upgrade twine
$ python3 -m twine upload --repository testpypi dist/*
```
