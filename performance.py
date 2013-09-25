"""
Log actions to Elastic Search (via UDP)
"""

import socket
import datetime
import json

try:
    from django.conf import settings as _django_settings
except ImportError as e:
    _django_settings = None


DEFAULT = {
    'host': 'localhost',
    'port': 9700,
    'protocol': 'UDP',
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
        'when': datetime.datetime.utcnow().isoformat()
    }

    if 'index' in kwargs.keys():
        index = kwargs['index']
        del kwargs['index']
    else:
        index = 'performance-' + datetime.date.today().strftime('%Y.%m.%d')

    for key, value in kwargs.iteritems():
        payload[key] = value

    emit(index, payload)

def emit(index, payload, host=None, port=None, protocol=None):
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
    if protocol == 'UDP':
        socket_type = socket.SOCK_DGRAM
    else:
        raise NotImplementedError(protocol)

    index_json_str = """{ "index": {"_index": "%s", "_type": "%s"} }""" %(index, "record")
    message =  "%s\n%s\n" % (index_json_str, json.dumps(payload))
    print message
    sock = socket.socket(socket.AF_INET, socket_type)
    sock.sendto(message,(host, port))
    sock.close()

if __name__ == '__main__':
    for i in range(10):
        log('somebody', 'Did something %sx times' % i)
