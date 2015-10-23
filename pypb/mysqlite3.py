# encoding: utf-8
"""
Simple wrappper for standard sqlite3 with certain options set in.
"""

import json
import sqlite3

Row = sqlite3.Row

# Boolean
sqlite3.register_adapter(bool, int)
# sqlite3.register_converter("boolean", bool)

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
# sqlite3.register_converter("json", json.loads)

def connect(database, *args, **kwargs):
    """
    Return a sqlite3 connection object with standard handlers.
    """

    # Set parsing of column and declaration type
    kwargs.setdefault("detect_types", sqlite3.PARSE_DECLTYPES)

    # Connect
    con = sqlite3.connect(database, *args, **kwargs)

    return con

def speedup(con,
            no_journal=True,
            set_page_size=False,
            cache_size=1024 * 512):
    """
    Setup some pragmas for sqlite3 speedup.
    """

    con.execute("pragma synchronous = off")
    con.execute("pragma cache_size = -%d" % cache_size)
    con.execute("pragma secure_delete = off")

    if no_journal:
        con.execute("pragma journal_mode = off")

    if set_page_size:
        con.execute("pragma page_size = 65536")
