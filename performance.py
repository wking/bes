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


### These have dumb names, but we might want them in some form? ###
def log_request(request):
    log(request.user, 'request', body=request.read())

def realy_quick_log(request):
    log(request.user, request.get_full_path())

def quick_log(request, what, **kwargs):
    log(request.user, what, **kwargs)



#-----------------------------------------------------------------------------#
# Real stuff
#-----------------------------------------------------------------------------#
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


def log(**kwargs):
    """Log an arbitrary payload dictionary to Elastic Search

    Uses the default connection configuration.  If you need to
    override any of them, build your payload dict by hand and use
    emit() instead.
    """
    kwargs['@timestamp'] = _datetime.datetime.utcnow().isoformat()
    kwargs['@version'] = 1
    emit(payload=kwargs)


def emit(payload, index=None, datestamp_index=None, type=None, **kwargs):
    """Send bulk-upload data to Elastic Search

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


if __name__ == '__main__':
    LOG.addHandler(_logging.StreamHandler())
    LOG.setLevel(_logging.DEBUG)

    for i in range(10):
        log(who='somebody', what='Did something %sx times' % i)
