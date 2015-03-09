# encoding: utf-8
"""
Read and write data to a text file atomically.

http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python

NOTE: Wont work on Non-POSIX systems
NOTE: Wont work with Python3
NOTE: Only supports read and write (not append or r+ or w+ modes)
"""

import __builtin__
import os
import os.path
from contextlib import contextmanager
from tempfile import NamedTemporaryFile as ntf

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

    # In read mode we have to do nothing
    if "r" in mode:
        return func(name, mode, *args, **kwargs)

    # Get the filename parts
    fname = os.path.abspath(name)
    prefix = os.path.basename(fname) + "-"
    dirname = os.path.dirname(fname)

    # Create the empty temporary file
    tobj = ntf(prefix=prefix, suffix=TMP_SUFFIX, dir=dirname, delete=False)
    tname = tobj.name
    tobj.close()

    # Reopen the file with proper func
    with func(tname, mode, *args, **kwargs) as fobj:
        yield fobj

        fobj.flush()
        os.fsync(fobj.fileno())

    # Now the atomic switch
    os.rename(tname, fname)

def open(*args, **kwargs):
    """
    Atomic context manager for open.
    """

    return _open(__builtin__.open, *args, **kwargs)

def gopen(*args, **kwargs):
    """
    Atomic context manager for gzip.open
    """

    return _open(gzip.open, *args, **kwargs)

def copen(*args, **kwargs):
    """
    Atomic context mangager for codecs.open
    """

    return _open(codecs.open, *args, **kwargs)

