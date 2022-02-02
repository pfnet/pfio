Tips
----

Q. PFIO is very slow with ``shutil.copyfileobj()``.
===================================================

A. Default buffer size of file objects from ``pfio.v2.S3`` is 16MB,
   while default size of ``shutil.COPY_BUFSIZE`` is 64 as of Python
   3.10. To avoid using default size, additional argument would help
   us align the buffer size:

.. code-block:: python

   import shutil
   from pfio.v2 import open_url
   
   with open_url('s3://bucket/very-large-file', 'wb') as dst:
     with open('very-large-local-file', 'rb') as src:
       shutil.copyfileobj(src, dst, length=16*1024*1024)
