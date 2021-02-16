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
