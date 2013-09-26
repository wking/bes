import re as _re
import unittest as _unittest
try:
    import unittest.mock as _mock
except ImportError:
    import mock as _mock

try:
    from django.conf import settings as _settings
except ImportError as e:
    _settings_error = e
    _settings = None
else:
    if not _settings.configured:
        _settings.configure(
            ELASTIC_SEARCH_LOGGING_PORT=9999,
            # for contrib.auth
            DATABASES={
                'default': {
                    'ENGINE': 'django.db.backends.dummy',
                    },
                },
            )
    try:
        import django.http as _http
        import django.contrib.auth.models as _auth

        import bes.django as _bes_django
    except ImportError as e:
        _settings_error = None
        _settings = None


DATE_TIME_REGEXP = _re.compile(
    r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[.]\d{6}')


def clean_message(message):
    if not hasattr(str, 'encode'):
        message = str(message, 'utf-8')

    search = DATE_TIME_REGEXP.search(message)
    if not search:
        raise ValueError('no timestamp in {!r}'.format(message))

    message = DATE_TIME_REGEXP.sub(
        'YYYY-MM-DDTHH:MM:SS.XXXXXX', message, count=1)
    return (message, search.group(0))


def skip_if_settings_is_none(obj):
    if _settings is None:
        return _unittest.skip(str(_settings_error))(obj)
    return obj


@skip_if_settings_is_none
class LogTestCase (_unittest.TestCase):
    def test_log_user_anonymous_request(self):
        request = _http.HttpRequest()
        request.user = _auth.AnonymousUser()
        message = _bes_django.log_user(request=request, sort_keys=True)
        message, timestamp = clean_message(message)
        self.assertEqual(
            message,
            b'\n'.join([
                b'{"index": {"_index": "log", "_type": "user-action"}}',
                b'{"@timestamp": "YYYY-MM-DDTHH:MM:SS.XXXXXX", "@version": 1, "user_id": null, "username": ""}',
                b'',
            ]))

    def test_log_user_request(self):
        request = _http.HttpRequest()
        request.user = _auth.User(id=123, username='jdoe')
        message = _bes_django.log_user(request=request, sort_keys=True)
        message, timestamp = clean_message(message)
        self.assertEqual(
            message,
            b'\n'.join([
                b'{"index": {"_index": "log", "_type": "user-action"}}',
                b'{"@timestamp": "YYYY-MM-DDTHH:MM:SS.XXXXXX", "@version": 1, "user_id": 123, "username": "jdoe"}',
                b'',
            ]))

    def test_log_user_request_override_type(self):
        request = _http.HttpRequest()
        request.user = _auth.User(id=123, username='jdoe')
        message = _bes_django.log_user(
            type='login', request=request, sort_keys=True)
        message, timestamp = clean_message(message)
        self.assertEqual(
            message,
            b'\n'.join([
                b'{"index": {"_index": "log", "_type": "login"}}',
                b'{"@timestamp": "YYYY-MM-DDTHH:MM:SS.XXXXXX", "@version": 1, "user_id": 123, "username": "jdoe"}',
                b'',
            ]))

    def test_log_user_request_additional_data(self):
        request = _http.HttpRequest()
        request.user = _auth.User(id=123, username='jdoe')
        message = _bes_django.log_user(
            request=request, module='testing', sort_keys=True)
        message, timestamp = clean_message(message)
        self.assertEqual(
            message,
            b'\n'.join([
                b'{"index": {"_index": "log", "_type": "user-action"}}',
                b'{"@timestamp": "YYYY-MM-DDTHH:MM:SS.XXXXXX", "@version": 1, "module": "testing", "user_id": 123, "username": "jdoe"}',
                b'',
            ]))

    def test_log_request_path(self):
        request = _http.HttpRequest()
        request.user = _auth.User(id=123, username='jdoe')
        request.path = '/music/bands/the_beatles/'
        request.META = {'QUERY_STRING': 'print=true'}
        message = _bes_django.log_request_path(request=request, sort_keys=True)
        message, timestamp = clean_message(message)
        self.assertEqual(
            message,
            b'\n'.join([
                b'{"index": {"_index": "log", "_type": "request"}}',
                b'{"@timestamp": "YYYY-MM-DDTHH:MM:SS.XXXXXX", "@version": 1, "request_path": "/music/bands/the_beatles/?print=true", "user_id": 123, "username": "jdoe"}',
                b'',
            ]))
