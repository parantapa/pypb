"""
Persistant data structures

This module creates pertsistant data structures list and set using sqlite3
as the underlying storage.
"""

from __future__ import division

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"

import sqlite3
import json
from collections import MutableMapping

def dumps(x):
    """
    Encode data to string.
    """

    return json.dumps([x])

def loads(x):
    """
    Decode data from string.
    """

    return json.loads(x)[0]

class Dict(MutableMapping):
    """
    Persistant sqlite3 dictionary

    If create_table is true (default), then the underlying table and index
    are created. The rest of the arguments are same as that of sqlite3.connect.
    """

    def __init__(self, dsn, create_table=True, create_index=True, *args, **kwargs):
        super(Dict, self).__init__()

        self.db     = sqlite3.connect(dsn, *args, **kwargs)
        self.cur    = self.db.cursor()

        if create_table:
            self.create_table()

        if create_index:
            self.create_index()

    def create_table(self):
        """
        Create the underlying data table.
        """

        sql = "create table if not exists dict (key text, value text)"
        self.cur.execute(sql)

    def create_index(self):
        """
        Create the underlying data index.
        """

        sql = "create unique index if not exists dict_key on dict (key)"
        self.cur.execute(sql)

    def drop_index(self):
        """
        Drop the underlying data index.
        """

        sql = "drop index if exists dict_key"
        self.cur.execute(sql)

    def commit(self):
        """
        Commit the underlying transaction.
        """

        self.db.commit()

    def rollback(self):
        """
        Rollback the underlying transaction.
        """

        self.db.rollback()

    def __enter__(self):
        self.db.__enter__()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.db.__exit__(exc_type, exc_value, exc_traceback)

    def dump(self):
        """
        Dump the context of the underlying database into stdout.

        To be used for debuging only.
        """

        sql = "select * from dict"
        self.cur.execute(sql)

        for row in self.cur:
            print repr(row)

    def __getitem__(self, k):
        key = dumps(k)

        sql = "select value from dict where key = ?"
        self.cur.execute(sql, (key,))
        value = self.cur.fetchone()
        if value is None:
            raise KeyError("Key {!r} doesn't exist".format(k))

        value = loads(value[0])
        return value

    def __setitem__(self, k, value):
        key   = dumps(k)
        value = dumps(value)

        sql = "insert or replace into dict values (?, ?)"
        self.cur.execute(sql, (key, value))

    def __delitem__(self, k):
        key = dumps(k)

        sql = "delete from dict where key = ?"
        self.cur.execute(sql, (key,))

        if self.cur.rowcount != 1:
            raise KeyError("Key {!r} doesn't exist".format(k))

    def __len__(self):
        sql = "select count(*) from dict"
        self.cur.execute(sql)
        return self.cur.fetchone()[0]

    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        sql = "select key from dict"
        cur = self.db.cursor()
        cur.execute(sql)
        return (loads(k) for k, in cur)

    def itervalues(self):
        sql = "select value from dict"
        cur = self.db.cursor()
        cur.execute(sql)
        return (loads(v) for v, in cur)

    def iteritems(self):
        sql = "select key, value from dict"
        cur = self.db.cursor()
        cur.execute(sql)
        return ((loads(k), loads(v)) for k, v in cur)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def clear(self):
        sql = "delete from dict"
        self.cur.execute(sql)

