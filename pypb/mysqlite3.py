# encoding: utf-8
"""
Simple wrappper for standard sqlite3 with certain options set in.
"""

import json
import sqlite3
import calendar
from datetime import datetime

# Boolean
sqlite3.register_adapter(bool, int)
sqlite3.register_converter("boolean", bool)

# Timestamp
sqlite3.register_converter("timestamp", int)

# List, Tuple, Dict
def dumps_compact(obj, _dumps=json.dumps):
    """
    Compact json dumper.
    """

    if not obj:
        return None

    return _dumps(obj, separators=(",",":"))

sqlite3.register_adapter(list, dumps_compact)
sqlite3.register_adapter(tuple, dumps_compact)
sqlite3.register_adapter(dict, dumps_compact)
sqlite3.register_converter("json", json.loads)

def connect(database, *args, **kwargs):
    """
    Return a sqlite3 connection object with standard handlers.
    """

    # Set parsing of column and declaration type
    kwargs.setdefault("detect_types", sqlite3.PARSE_DECLTYPES)

    # Connect
    con = sqlite3.connect(database, *args, **kwargs)

    return con

def twitter_timestamp(text,
                      _strptime=datetime.strptime,
                      _timegm=calendar.timegm):
    """
    Convert the Twitter time string to timestamp.
    """

    if text is None:
        return None

    d = _strptime(text, "%a %b %d %H:%M:%S +0000 %Y")
    d = d.timetuple()
    return _timegm(d)

