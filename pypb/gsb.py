"""
Python API for Google Safe Browsing Lookup API
"""

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"
__version__ = 0.1

from collections import defaultdict

import pypb.req as req

CLIENT = "pypb.gsb"
PVER   = 3.0
URL    = "https://sb-ssl.google.com/safebrowsing/api/lookup"

from logbook import Logger
log = Logger(__name__)

class Error(Exception):
    """
    Google safe browsing exception
    """

    pass

def lookup(key, urls):
    """
    Check ecah url using Google Safe Browsing Lookup API.

    Returns a dict mapping each url to the api response.

    key  - API Key
    urls - List of urls to check
    """

    # Generate the get params
    params = {"client": CLIENT,
              "appver": __version__,
              "apikey": key,
              "pver"  : PVER}

    # Generate the post body
    data   = str(len(urls)) + "\n" + "\n".join(urls)

    while True:
        # Make the request
        r = req.post(URL, params=params, data=data)

        # We have at least one match
        if r.status_code == 200:
            ret = r.text
            ret = ret.split("\n")
            ret = dict(zip(urls, ret))
            return ret

        # We have no match
        if r.status_code == 204:
            return defaultdict(lambda: "ok")

        # Unexpected server response
        errmsg = {400: "Bad Request",
                  401: "Not Authorized"}
        errmsg = errmsg.get(r.status_code, "Unknown")

        msg = "{}: {}\nkey={}\n{}"
        msg = msg.format(r.status_code, errmsg, key, "\n".join(urls))
        raise Error(msg)

