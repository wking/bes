try:
    import wrapt as _wrapt
except ImportError as e:
    _wrapt = None
    import functools as _functools


import bes as _bes


def trace(start=True, error=True, complete=True, logger=_bes.log,
          **log_kwargs):
    """A decorator to get automatic bes logging for function execution

    Use::

      @trace()
      def your_task():
          ...

    To log start and completion of your task with bes.
    """
    # closure to capture start, logger, etc. and run the logging
    def log_wrapper(wrapped, *args, **kwargs):
        if 'type' not in log_kwargs:
            log_kwargs['type'] = wrapped.__name__
        if start:
            logger(action='start', **log_kwargs)
        try:
            result = wrapped(*args, **kwargs)
        except Exception as e:
            if error:
                logger(action='error', error=str(e), **log_kwargs)
            raise
        else:
            if complete:
                logger(action='complete', **log_kwargs)
        return result

    if _wrapt:
        @_wrapt.decorator
        def decorator(wrapped, instance, args, kwargs):
            return log_wrapper(wrapped, *args, **kwargs)
    else:
        def decorator(wrapped):
            @_functools.wraps(wrapped)
            def wrapper(*args, **kwargs):
                return log_wrapper(wrapped, *args, **kwargs)
            return wrapper

    return decorator
