.. module:: pfio

API Reference
=============

Toplevel Functions
------------------

.. note:: Toplevel functions will be deprecated in 2.0 and removed in
          2.1. Please use V2 API instead.

.. autofunction:: open
.. autofunction:: open_as_container
.. autofunction:: list
.. autofunction:: create_handler
.. autofunction:: info
.. autofunction:: isdir
.. autofunction:: mkdir
.. autofunction:: makedirs
.. autofunction:: exists
.. autofunction:: rename
.. autofunction:: remove

.. autofunction:: set_root
.. autofunction:: get_root_dir

.. autoclass:: pfio.io.FileStat
   :members:

.. autoclass:: IO
   :members:
.. autoclass:: pfio.filesystem.FileSystem
   :members:
.. autoclass:: pfio.container.Container
   :members:

.. note:: With environment variable
          ``KRB5_KTNAME=path/to/your.keytab`` set, ``hdfs``
          handler automatically starts automatic and periodical
          updating Kerberos ticket using `krbticket
          <https://pypi.org/project/krbticket/>`_ . The update
          frequency is every 10 minutes by default.
.. note::
          Only the username in the first entry in The
          keytab will be used to update the Kerberos ticket.


Cache API
---------

.. currentmodule:: pfio.cache

PFIO provides experimental cache API to improve performance of
repetitive access to the data collection.

.. admonition:: Example

   Here let us suppose we have a file that includes a list of paths to images.
   ::

       /path/to/image1.jpg
       /path/to/image2.jpg
       ...
       /path/to/imageN.jpg

   The PyTorch Dataset class with using :class:`~NaiveCache` as an example
   can be implemented as follows.
   ::

       from pfio.cache import NaiveCache


       class MyDataset(torch.utils.data.Dataset):
           def __init__(self, image_paths):
               self.paths = image_paths
               self.cache = NaiveCache(len(image_paths), do_pickle=True)

           def __len__(self):
               return len(self.paths)

           def _read_image(self, i):
               return cv2.imread(self.paths[i]).transpose(2, 0, 1)

           def __getitem__(self, i):
               x = self.cache.get_and_cache(i, self._read_image)

               # This is equivalent
               # x = self.cache.get(i)
               # if not x:
               #     x = cv2.imread(self.paths[i]).transpose(2, 0, 1)
               #     self.cache.put(i, x)

               return torch.Tensor(x)

   By calling ``get_and_cache`` of the cache in ``__getitem__`` method,
   it will check if the data for the specified index is already cached.
   If there already is, it reads the data from the cache and return,
   otherwise it calls the actual data loading function, add it to the cache,
   and return it.
   Therefore load the data from the storage only when necessary,
   which is at the first access to each data.

PFIO cache API provides :class:`~NaiveCache`, :class:`~FileCache`and
:class:`~MultiprocessFileCache`.
They all share the same core idea and interface.
The difference is how to manage the cached data.

The :class:`~NaiveCache` keeps everything in memory,
making it virtually zero overhead.
The cache capacity is limited by the memory size,
thus it would not be suitable for large-scale datasets.

The :class:`~FileCache` and the :class:`~MultiprocessFileCache` both
store the cached data in a filesystem.
The :class:`~FileCache` is designed for single-process data load.
In case of parallelized data loading, which is relatively common in
deep learning workloads, consider using :class:`~MultiprocessFileCache`.

Also, these file-based caches support cache data persistency.
Once the cache is completely built, we can keep them as files by calling
:func:``FileCache.preserve``, and we can recover the cache
from the preserved files by calling :func:``FileCache.preload``.
This is useful when we want to reuse the cache already built in a previous workload.

Currently deletion of a data from cache is not supported.


.. autoclass:: Cache
   :members:

.. autoclass:: NaiveCache
   :members:

.. autoclass:: FileCache
   :members: preserve, preload

.. autoclass:: MultiprocessFileCache
   :members: preserve, preload
