from __future__ import absolute_import

from django.conf import settings as _django_settings

import bes as _bes


if _django_settings.configured:
    for key,value in _bes.DEFAULT.items():
        django_key = 'BULK_ELASTIC_SEARCH_LOGGING_{}'.format(key.upper())
        _bes.DEFAULT[key] = getattr(_django_settings, django_key, value)


def log_user(type='user-action', request=None, **kwargs):
    """Log activity requested by a Django user

    This uses request.user.id and request.user.username.  That's what
    you get with the default contrib.auth.User, but likely you'll have
    them even if you override AUTH_USER_MODEL.

    https://docs.djangoproject.com/en/dev/ref/request-response/
    https://docs.djangoproject.com/en/dev/topics/auth/default/#user-objects
    """
    return _bes.log(
        type=type,
        user_id=request.user.id,
        username=request.user.username,
        **kwargs)


def log_request_path(type='request', request=None, **kwargs):
    """Like log_user, but also adds the request path
    """
    return log_user(
        type=type,
        request=request,
        request_path=request.get_full_path(),
        **kwargs)


def log_request_body(request=None, **kwargs):
    """Like log_request_path, but also adds the request body
    """
    return log_request_path(
        request=request,
        request_body=request.read(),
        **kwargs)
