"""
Log actions to Elastic Search (via UDP)
"""

import datetime as _datetime
import json as _json
import logging as _logging
import socket as _socket

try:
    from django.conf import settings as _django_settings
except ImportError as e:
    _django_settings = None


LOG = _logging.getLogger(__name__)


DEFAULT = {
    'host': 'localhost',
    'port': 9700,
    'protocol': 'UDP',
    'index': 'log',
    'datestamp_index': False,
    'type': 'record',
    }


if _django_settings and _django_settings.configured:
    for key,value in DEFAULT.items():
        django_key = 'ELASTIC_SEARCH_LOGGING_{}'.format(key.upper())
        DEFAULT[key] = getattr(_django_settings, django_key, value)


class Connection(object):
    """A socket connecting to Elastic Search

    Use a context manager for PEP 343's 'with' syntax:

    >>> with Connection(host='localhost', port=1234) as c:
    ...     c.send(message='hello!')
    """
    def __init__(self, host=None, port=None, protocol=None):
        if host is None:
            host = DEFAULT['host']
        if port is None:
            port = DEFAULT['port']
        if protocol is None:
            protocol = DEFAULT['protocol']
        self.host = host
        self.port = port
        if protocol == 'UDP':
            self.socket_type = _socket.SOCK_DGRAM
        else:
            raise NotImplementedError(protocol)
        self._sock = None

    def __enter__(self):
        self._sock = _socket.socket(_socket.AF_INET, self.socket_type)
        return self

    def __exit__(self, *exc_info):
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def send(self, message):
        LOG.debug(message)
        self._sock.sendto(message, (self.host, self.port))


def log(index=None, type=None, **kwargs):
    """Log an arbitrary payload dictionary to Elastic Search

    Uses the default connection configuration.  If you need to
    override any of them, build your payload dict by hand and use
    emit() instead.

    You can optionally override the index and type of payload, for
    later filtering in Elastic Search.  This means that `index` and
    `type` are not available as payload keys.
    """
    kwargs['@timestamp'] = _datetime.datetime.utcnow().isoformat()
    kwargs['@version'] = 1
    emit(payload=kwargs, index=index, type=type)


def emit(payload, index=None, datestamp_index=None, type=None, **kwargs):
    """Send bulk-upload data to Elastic Search

    Uses the 'index' action to add or replace a document as necessary.

    http://www.elasticsearch.org/guide/reference/api/bulk/
    http://www.elasticsearch.org/guide/reference/api/bulk-udp/
    """
    #TODO indexes, types, and what Kibana likes.
    #Try it out and adjust
    #throwing all of payloads **kwargs into an 'additional' or simular field might 
    #required, and I don't know what happens if we send different data types with
    #the same name ie a **kwargs of my_special_key: str and my_special_key: {'foo': 'bar'}
    if index is None:
        index = DEFAULT['index']
    if type is None:
        type = DEFAULT['type']
    if datestamp_index is None:
        datestamp_index = DEFAULT['datestamp_index']
    if datestamp_index:
        index = '-'.join([
            index,
            _datetime.date.today().strftime('%Y.%m.%d'),
            ])

    index_data = {
        'index': {
            '_index': index,
            '_type': type,
            },
        }
    message = '\n'.join([
        _json.dumps(index_data),
        _json.dumps(payload),
        '',
        ])

    with Connection(**kwargs) as connection:
        connection.send(message)


def log_django_user(type='user-action', request=None, **kwargs):
    """Log activity requested by a Django user

    This uses request.user.id and request.user.username.  That's what
    you get with the default contrib.auth.User, but likely you'll have
    them even if you override AUTH_USER_MODEL.

    https://docs.djangoproject.com/en/dev/ref/request-response/
    https://docs.djangoproject.com/en/dev/topics/auth/default/#user-objects
    """
    log(
        type=type,
        user_id=request.user.id,
        username=request.user.username,
        **kwargs)


def log_django_request_path(type='request', request=None, **kwargs):
    """Like log_django_user, but also adds the request path
    """
    log_django_user(
        type=type,
        request=request,
        request_path=request.get_full_path(),
        **kwargs)


def log_django_request_body(request=None, **kwargs):
    """Like log_django_request_path, but also adds the request body
    """
    log_django_request_path(
        request=request,
        request_body=request.read(),
        **kwargs)


if __name__ == '__main__':
    LOG.addHandler(_logging.StreamHandler())
    LOG.setLevel(_logging.DEBUG)

    for i in range(3):
        log(who='somebody', what='Did something %sx times' % i)
