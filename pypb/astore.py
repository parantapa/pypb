# encoding: utf-8
"""
Read and write data to a text file atomically.

http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python
"""

import __builtin__
import os
from contextlib import contextmanager

import gzip
import codecs

TMP_SUFFIX = ".astore-tmp"

@contextmanager
def _open(func, name, mode="rb", *args, **kwargs):
    """
    Make sure the data is written atomically.
    """

    # Xor of the two conditions
    assert ("w" in mode) != ("r" in mode)

    # Which file to operate on?
    if "r" in mode:
        fname = name
    else: # w in mode
        fname = name + TMP_SUFFIX

    with func(fname, mode, *args, **kwargs) as fobj:
        yield fobj

        # In case of write flush and sync
        if "w" in mode:
            fobj.flush()
            os.fsync(fobj.fileno())

    # In case of write: do atomic switch
    if "w" in mode:
        os.rename(fname, name)

def open(*args, **kwargs):
    """
    Atomic context manager for open.
    """

    return _open(__builtin__.open, *args, **kwargs)

def gzip_open(*args, **kwargs):
    """
    Atomic context manager for gzip.open
    """

    return _open(gzip.open, *args, **kwargs)

def codecs_open(*args, **kwargs):
    """
    Atomic context mangager for codecs.open
    """

    return _open(codecs.open, *args, **kwargs)

