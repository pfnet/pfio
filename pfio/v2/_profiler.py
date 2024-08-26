try:
    from pytorch_pfn_extras.profiler import record, record_iterable

except ImportError:

    class _DummyRecord:
        def __init__(self):
            pass

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_value, traceback):
            pass

    # IF PPE is not available, wrap with noop
    def record(tag, trace, *args):  # type: ignore # NOQA
        return _DummyRecord()

    def record_iterable(tag, iter, trace, *args):   # type: ignore # NOQA
        yield from iter
