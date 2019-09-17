Design
------

ChainerIO is an IO abstraction library for Chainer, optimized for deep
learning training with batteries included. It supports

- Filesystem API abstraction with unified error semantics,
- Explicit user-land caching system,
- IO performance tracing and metrics stats, and
- Fileset container utilities to save metadata.


Rationale
+++++++++

There are a lot of non-POSIX-compliant filesystems in the industry and
all of them have Pros and Cons, from cloud storages like Google Cloud
Stroage and Amazon S3, to on-premise distributed filesystems like
HDFS. Supporting different filesystems by developers themselves
creates unnecessary burden on the developers and might reduce the
portability on the code. That is the motivation of ChainerIO
supporting *filesystem abstraction API*.

Also, deep learning training programs have benefited from filesystem
page caching provided Linux kernel because its optmization method is
based on stochastic gradient descent, where all training data is
repeatedly read to iteratively train the model. But such
non-POSIX-compliant filesystem usually does not provide content
caching capability and I/O workload of training program would not
implicitly optmized unlike Posix-based filesystems in Linux.  To help
with learning on non-POSIX-compliant filesystems, e.g. HDFS, ChainerIO
implements *data caching capability in userland*. Moreover, developers
can choose which data to cache, from the raw files to DNN input NumPy
array.


Opmization in deep learning model training is also important as the
model training usually takes long time and even 1% speedup is
important. Modern external non-Posix filesystem is based on
complex communication protocol between multiple data nodes and its
performance metrics are not simply observable. *Built-in tracing
system,* would be the first step for optimization, mitigating the
complexity problem.

One of the general problems in filesystems is scalability of metadata
access, caused by inbalanced ratio of number of files and total
capacity of one system. ChainerIO supports various *container file
formats to aggregate many small files into single large file* with
metadata mapping, e.g. HDF5, ZIP and Tar (and more in future), by
taking advantage of the fact that in machine learning training
workload, usually SGD, is repeated read of single fixed dataset
(Write-once-and-read-many). Aggregating millions of single-kilobytes
files into single file would save a lot of metadata store.

Architecture
++++++++++++

ChainerIO abstracts underlying system with three objects. See API
documentation for details.



.. image:: _static/overview.png
   :alt: Design Overview
   :width: 80%
   :align: center


FileSystem
~~~~~~~~~~

Abstraction of each filesystems. Depending on the context the term
might stand for the filesystem type, or the (network) filesystem
instance. It supports

- Getting basic information of the filesystem (info)
- Container creation, deletion
- Accessing containers (open_as_container)
- Accessing raw files (open)
- Listing all files under specific directory
- Primarily HDFS and POSIX

.. code-block:: python

    import chainerio
    # Create Filesystem Accessor Object
    with chainerio.create_handler('hdfs://name-service1/') as handler:
        print(handler.info())
        # TODO(open mode) actually 'r' is not support by hdfs
        # neither is readlines
        # but we can use wrapper
        with handler.open('some/file.txt', 'r') as fp:
            print(fp.readlines())

        with handler.open_as_container('some/container-name.zip') as container:
            print(container.info())

        # Files in a directory can be listed with ``list`` method
        for name in handler.list('path/to/dir'):
            ...

Filesystem Context
~~~~~~~~~~~~~~~~~~

ChainerIO also provides a set of simpler API set using process-wide
filesystem context. The context includes target filesystem type and
service instance, and opened container.

In spite of its simplicity, developers should be aware as the results
rely on the state of the context, e.g. the current filesystem or
service instance. The default setting is local filesystem.

.. code-block:: python

    import chainerio

    # Same as Python's built-in ``open()`` effectively
    with chainerio.open('local-file.txt') as fp:
        ...

    # Set default context globally in this process
    chainerio.set_root('hdfs://name-service-cluster1/')

    # Opens ``some/file.txt`` in HDFS name-service-cluster1,
    # relative path from home directory in HDFS
    with chainerio.open('some/file.txt', 'r') as fp:
        for line in fp.readlines():
            print(line)

    # Opening container also refers to the default root
    with chainerio.open_as_container('some/container.zip') as container:
        for name in container.list():
            print(name)




Containers
~~~~~~~~~~

Abstraction of file containers such as ZIP. It contains a set of (key,
binary object) pairs. Keys are typically path-like string and binary
is typically a file content. In ChainerIO keys in a container are
UTF-8 strings. Containers can be nested, e.g. ZIP in ZIP. It supports:

- Showing basic information of the container (info)
- Accessing raw files included (open)
- Accessing containers included (open)
- Adding and remove file (create, delete)
- Listing keys in a container
- Primarily ZIP, and possibly Hdf5?

.. code-block:: python

    import chainerio
    from PIL import Image
    import io

    chainerio.set_root('hdfs://name-service-cluster1/')

    with chainerio.open_as_container('some/many-files-dataset.zip') as container:
        print(container.info())
        # List all keys in the container
        for name in container.namelist():
            print(name)

        # Obtains a file object to access binary content that
        # corresponds to the key ``some/file.jpg``
        with container.open('some/file.jpg', 'rb') as fp:
            binary = fp.read()
            image = Image(io.BytesIO(binary))
            ...


Containers can also be registered as default context and can behave
virtually as a filesystem.

Containers can be also a root context with ``set_root`` method:

.. code-block:: python

    import chainerio
    root_container = chainerio.open_as_container('some/important/container.zip')
    # Same as fs.set_root('some/important/container.zip')
    chainerio.set_root(root_container)

    # Opens a file contained in ``some/important/container.zip``
    with chainerio.open('some/file.jpg', 'rb') as fp:
        ...

    # Iterates over names that matches the prefix
    for name chainerio.list('some/'):
        ...

File-like Objects
~~~~~~~~~~~~~~~~~

Abstraction of binary objects or files, typically returned by
``fs.open`` method. It is an implementation of ``RawIOBase`` class
(See `RawIOBase
<https://docs.python.org/3/library/io.html#io.RawIOBase>`__ in Python
document). It supports

- Read to underlying file or binary in a container
- Writes supported by filesystems, but possibly not in containers


URI Expression of File Paths
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Filesystems can be expressed as::

   <filesystem> := <scheme>[://<service-idenfier>]

where ``scheme`` represends the filesystem type. Currently ``hdfs``
and ``file`` are supported. ``hdfs`` stands for HDFS and ``file``
means local filesystem. For remote network file system like HDFS,
``service-identifier`` stands for service instance. It can be omitted
when the default service is defined. For example in HDFS, it is the
name of name service described in ``hdfs-site.xml`` in the Hadoop
configuration directory like following example::

  <configuration>
    <property>
      <name>dfs.nameservices</name>
      <value>hdfs-nameservice1</value>
    </property>
    ...

In this example is ``service-identifier`` is ``hdfs-nameservice``.


Containers, files are uniquely identifiable by partial
set of URI expression::

   <uri> := <scheme>://[<service-idenfier>]/<path>

``service-identifier`` can be omitted when it can be uniquely defined
by the environment. ``path`` is a UTF-8 string, a sequence of path
segments separated by ``/`` and path segments are recommended to only
use ``[a-z][A-Z][0-9][_-]`` . However, details depend on underlying filesystem
implementation or containers.

``chainerio.open_as_container`` and ``chainerio.fs.open`` take
``filesystem``, ``uri`` or ``path`` as an argument to identify the
file to be opened, when the context is a filesystem.  If the context
is a container, they accept a key as an argument.

If the context is a file system, they also take a ``path`` as a
relative path. The base for relative path depends on filesystems; for
HDFS it is home directory and for POSIX it is current working
directory.

For example, all these ``fs.open`` open the same file, given that the
default name service is ``name-service1`` and user Smith's home
derectory is defined as ``/user/smith``:

.. code-block:: python

    import chainerio

    # Using full URI
    chainerio.open('hdfs://name-service1/user/smith/path/to/file.txt')

    # Using set_root and absolute path
    chainerio.set_root('hdfs://name-service1/')
    chainerio.open('/user/smith/path/to/file.txt')

    # Using set root and relative path
    chainerio.set_root('hdfs')
    chainerio.open('path/to/file.txt')

    # Overwrite the global setting with full URI
    # Access the posix with the global setting to hdfs
    chainerio.open('file://path/to/file.txt')

    # Accessing with filesystem object
    with chainerio.create_handler('hdfs') as handler:
        handler.open('file.txt')


Major Use Cases
++++++++++++++++

With all these primitive concepts and operations ChainerIO supports
various use cases from loading training data, taking snapshots of
models in the middle of training process, and recording the final
model.

In order to load training data in Chainer, developers create a
`dataset` class which derived the `DatasetMixin` from the
`chainer.dataset` package. ChainerIO will provide several
implementation replacements for generic datasets included in Chainer
and other Chainer family libraries.


According to the survey we conduct on developers' code. I/Os can be
categorized into two different classes.

1. Inputs and outputs using file object: direct access via
   built-in APIs e.g. `Image` class in PIL, `cv2.image.open` and
   `pandas.read_hdf`.  In such case, the file object (in ChainerIO, it
   is implementation of `RawIOBase
   <https://docs.python.org/3/library/io.html#raw-i-o>`_ )


2. Inputs and outputs all wrapped by 3rd party library. Some of them
   has functions only takes the file path string as an argument and
   all file operations are hidden underneath the library. Examples are
   `cv2.VideoWriter()`, `cv2.imread()` and `cv2.VideoCapture()` from
   OpenCV. Since we cannot change the library, we provide a monkey
   patch of major libraries frequently used along with Chainer.

For details see API.
