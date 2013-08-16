"""
A simple and robust way for making http requests.

This library wraps python-requests. The errors handled by this module are
connection requests and server side errors. The error handling strategy
is quite simple. Wait for fixed number of seconds on error and then retry.
"""

import requests
from time import sleep

GIVE_UP_AFTER = 24 * 3600
RETRY_AFTER   = 10
RETRY_MAX     = GIVE_UP_AFTER // RETRY_AFTER

from logbook import Logger
log = Logger(__name__)

class Error(Exception):
    """
    Raised on error cases.
    """

def robust_http(args, kwargs, method):
    """
    Repeat the HTTP GET/POST operatopn in case of failure.
    """

    # Keep trying for downlod
    for tries in xrange(RETRY_MAX):
        try:
            return method(*args, **kwargs)
        except requests.RequestException as e:
            msg = u"Try L0: {} - Get Failed: {}: {}"
            log.info(msg, tries, type(e), e)
            sleep(RETRY_AFTER)
        except Exception as e:
            msg = u"Try L0: {} - Get Failed: {}: {}"
            log.warn(msg, tries, type(e), e)
            sleep(RETRY_AFTER)

    # Cant help any more; Quit program
    msg = "Falied to make http request!"
    log.critical(msg)
    raise Error(msg)

def checked_http(args, kwargs, method):
    """
    Check the status codes and repeat on server side errors.
    """

    accept_codes = kwargs.pop("accept_codes", ())

    # Keep trying for until you get acceptable response
    for tries in xrange(RETRY_MAX):
        r = robust_http(args, kwargs, method)

        # Good response
        if 200 <= r.status_code < 300 or r.status_code in accept_codes:
            return r

        # Bad client request, can't handle here
        if 400 <= r.status_code < 500:
            msg = u"Try L1: {}, Code {}: Client Error: {}"
            log.info(msg, tries, r.status_code, r.text)
            return r

        # Server problem retry
        if 500 <= r.status_code < 600:
            msg = u"Try L1: {}, Code {}: Server Error: {}"
            log.info(msg, tries, r.status_code, r.text)
            sleep(RETRY_AFTER)
            continue

        # Unknown response
        msg = u"Try L1: {}, Code {}: Unexpected Error: {}"
        log.warn(msg, tries, r.status_code, r.text)
        sleep(RETRY_AFTER)
        continue

    # Cant help any more; Quit program
    msg = "Server kept failing!"
    log.critical(msg)
    raise Error(msg)

def get(*args, **kwargs):
    """
    Perform a robust GET request.
    """

    session = kwargs.get("session", None)
    if session is None:
        return checked_http(args, kwargs, requests.get)
    else:
        return checked_http(args, kwargs, session.get)

def post(*args, **kwargs):
    """
    Perform a robust POST request.
    """

    session = kwargs.get("session", None)
    if session is None:
        return checked_http(args, kwargs, requests.post)
    else:
        return checked_http(args, kwargs, session.post)

