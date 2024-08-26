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

Q. How to output tracer results (readable by Chrome Tracing).
=============================================================

A. As shown in the sample program below,
   by executing `initialize_writer()` and `flush()` after tracing,
   JSON that can be read by Chrome tracing can be output.

.. code-block:: python

   import json
   import pytorch_pfn_extras as ppe

   from pfio.v2 import Local, Path, from_url

   tracer = ppe.profiler.get_tracer()

   with Local(trace=True) as fs:
     for f in fs.list():
       if fs.isdir(f.strip()):
         dir += 1
         continue
            
       fil += 1
       len(fs.open(f).read())

   w = ppe.writing.SimpleWriter(out_dir="")

   # output '['
   tracer.initialize_writer("trace.json", w)
   # output json dump
   tracer.flush("trace.json", w)