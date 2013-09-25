"""
Log actions to Elastic Search (via UDP)
"""

import socket
import datetime
import json

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

def emit(index, payload):
    """
    This function broadcasts data via UDP to ElasticSearch
    """
    #TODO indexes, types, and what Kibana likes.
    #Try it out and adjust
    #throwing all of payloads **kwargs into an 'additional' or simular field might 
    #required, and I don't know what happens if we send different data types with
    #the same name ie a **kwargs of my_special_key: str and my_special_key: {'foo': 'bar'}

    try:
        host = settings.PERFORMANCE_LOGGING_HOST
        port = settings.PERFORMANCE_LOGGING_PORT
    except NameError:
        host = '127.0.0.1'
        port = 9700

    index_json_str = """{ "index": {"_index": "%s", "_type": "%s"} }""" %(index, "record")
    message =  "%s\n%s\n" % (index_json_str, json.dumps(payload))
    print message
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(message,(host, port))
    sock.close()

if __name__ == '__main__':
    for i in range(10):
        log('somebody', 'Did something %sx times' % i)
