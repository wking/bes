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
def log(who, what, **kwargs):
    """
    This function combines in arguments, possibly setting an index,
    adds a consistant timestamp and hands it off to ElasticSearch
    """
    payload = {
        'who': who,
        'what': what,
        'when': _datetime.datetime.utcnow().isoformat()
    }

    for key, value in kwargs.iteritems():
        payload[key] = value

    emit(payload=payload, **kwargs)


def emit(payload, host=None, port=None, protocol=None,
         index=None, datestamp_index=None, type=None):
    """
    This function send data to ElasticSearch
    """
    #TODO indexes, types, and what Kibana likes.
    #Try it out and adjust
    #throwing all of payloads **kwargs into an 'additional' or simular field might 
    #required, and I don't know what happens if we send different data types with
    #the same name ie a **kwargs of my_special_key: str and my_special_key: {'foo': 'bar'}
    if host is None:
        host = DEFAULT['host']
    if port is None:
        port = DEFAULT['port']
    if protocol is None:
        protocol = DEFAULT['protocol']
    if index is None:
        index = DEFAULT['index']
    if type is None:
        type = DEFAULT['type']
    if datestamp_index is None:
        datestamp_index = DEFAULT['datestamp_index']
    if protocol == 'UDP':
        socket_type = _socket.SOCK_DGRAM
    else:
        raise NotImplementedError(protocol)
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

    LOG.debug(message)
    sock = _socket.socket(_socket.AF_INET, socket_type)
    sock.sendto(message,(host, port))
    sock.close()


if __name__ == '__main__':
    LOG.addHandler(_logging.StreamHandler())
    LOG.setLevel(_logging.DEBUG)

    for i in range(10):
        log(who='somebody', what='Did something %sx times' % i)
