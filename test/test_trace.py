import inspect as _inspect
import sys as _sys
import unittest as _unittest

try:
    import wrapt as _wrapt
except ImportError as e:
    _wrapt = None
    _wrapt_error = e

import bes.trace as _bes_trace


def skip_if_missing_wrapt(obj):
    if _wrapt is None:
        return _unittest.skip(str(_wrapt_error))(obj)
    return obj


def skip_if_python_version_less_than(version):
    skip = _sys.version_info < version
    def decorator(obj):
        if skip:
            return _unittest.skip(str(_wrapt_error))(obj)
        return obj
    return decorator


class Logger (object):
    def __init__(self):
        self.messages = []

    def __call__(self, *args, **kwargs):
        self.messages.append({'args': args, 'kwargs': kwargs})


class TraceTestCase (_unittest.TestCase):
    def test_trace(self):
        logger = Logger()

        @_bes_trace.trace(logger=logger)
        def foo(*args, **kwargs):
            logger('running')

        foo(1, 'a', b=3)

        self.assertEqual(
            logger.messages,
            [
                {
                    'args': (),
                    'kwargs': {
                        'action': 'start',
                        'type': 'foo',
                        },
                    },
                {
                    'args': (
                        'running',
                        ),
                    'kwargs': {},
                    },
                {
                    'args': (),
                    'kwargs': {
                        'action': 'complete',
                        'type': 'foo',
                        },
                    },
                ])

    def test_with_arguments(self):
        logger = Logger()

        @_bes_trace.trace(logger=logger, a=1)
        def foo(*args, **kwargs):
            logger('running')

        foo(1, 'a', b=3)

        self.assertEqual(
            logger.messages,
            [
                {
                    'args': (),
                    'kwargs': {
                        'a': 1,
                        'action': 'start',
                        'type': 'foo',
                        },
                    },
                {
                    'args': (
                        'running',
                        ),
                    'kwargs': {},
                    },
                {
                    'args': (),
                    'kwargs': {
                        'a': 1,
                        'action': 'complete',
                        'type': 'foo',
                        },
                    },
                ])

    def test_with_explicit_type(self):
        logger = Logger()

        @_bes_trace.trace(type='my-type', logger=logger, a=1)
        def foo(*args, **kwargs):
            logger('running')

        foo(1, 'a', b=3)

        self.assertEqual(
            logger.messages,
            [
                {
                    'args': (),
                    'kwargs': {
                        'a': 1,
                        'action': 'start',
                        'type': 'my-type',
                        },
                    },
                {
                    'args': (
                        'running',
                        ),
                    'kwargs': {},
                    },
                {
                    'args': (),
                    'kwargs': {
                        'a': 1,
                        'action': 'complete',
                        'type': 'my-type',
                        },
                    },
                ])

    def test_with_error(self):
        logger = Logger()

        @_bes_trace.trace(logger=logger, a=1)
        def foo(*args, **kwargs):
            raise ValueError('dying')

        self.assertRaises(ValueError, foo, 1, 'a', b=3)
        self.assertEqual(
            logger.messages,
            [
                {
                    'args': (),
                    'kwargs': {
                        'a': 1,
                        'action': 'start',
                        'type': 'foo',
                        },
                    },
                {
                    'args': (),
                    'kwargs': {
                        'a': 1,
                        'action': 'error',
                        'error': 'dying',
                        'type': 'foo',
                        },
                    },
                ])

    def test_name(self):
        @_bes_trace.trace()
        def foo(*args, **kwargs):
            return 1

        self.assertEqual(foo.__name__, 'foo')

    def test_doc(self):
        @_bes_trace.trace()
        def foo(*args, **kwargs):
            'A test method'
            return 1

        self.assertEqual(foo.__doc__, 'A test method')

    @skip_if_python_version_less_than(version=(3, 3))
    def test_signature(self):
        @_bes_trace.trace()
        def foo(a, b=3, *args, **kwargs):
            'A test method'
            return 1

        if hasattr(_inspect, 'signature'):  # Python >= 3.3
            signature = _inspect.signature(foo)
            self.assertEqual(
                str(signature), '(a, b=3, *args, **kwargs)')
        else:
            if hasattr(_inspect, 'getfullargspec'):  # Python 3
                argspec = _inspect.getfullargspec(foo)
                self.assertEqual(argspec.varkw, 'kwargs')
                self.assertEqual(argspec.kwonlyargs, [])
                self.assertEqual(argspec.kwonlydefaults, None)
                self.assertEqual(argspec.annotations, {})
            else:  # Python 2
                argspec = _inspect.getargspec(foo)
                self.assertEqual(argspec.keywords, 'kwargs')
            self.assertEqual(argspec.args, ['a', 'b'])
            self.assertEqual(argspec.varargs, 'args')
            self.assertEqual(argspec.defaults, (3,))

    @skip_if_missing_wrapt
    def test_code(self):
        @_bes_trace.trace()
        def foo(a, b=3, *args, **kwargs):
            """A test method"""
            return 1 * 2 + 3

        source = _inspect.getsource(foo)
        self.assertEqual(
            source,
            '\n'.join([
                '        @_bes_trace.trace()',
                '        def foo(a, b=3, *args, **kwargs):',
                '            """A test method"""',
                '            return 1 * 2 + 3',
                '',
                ]))
