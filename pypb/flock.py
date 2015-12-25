# encoding: utf-8
"""
Simple file lock context manager.

NOTE: Wont work on Non-POSIX systems
NOTE: Wont work with Python3
"""

import __builtin__
import fcntl
from contextlib import contextmanager

import gzip
import codecs

@contextmanager
def locked_opener(func, name, *args, **kwargs):
    """
    Make a blocking lock.
    """

    lock_type = kwargs.pop("lock_type", "exclusive")
    blocking = kwargs.pop("blocking", True)

    if lock_type == "exclusive":
        operation = fcntl.LOCK_EX
    elif lock_type == "shared":
        operation = fcntl.LOCK_SH
    else:
        raise ValueError("Invalid lock_type: '%s'" % lock_type)

    if not blocking:
        operation = operation | fcntl.LOCK_NB

    with func(name, *args, **kwargs) as fobj:
        fd = fobj.fileno()
        fcntl.lockf(fd, operation)

        yield fobj

        fcntl.lockf(fd, fcntl.LOCK_UN)

def open(*args, **kwargs): # pylint: disable=redefined-builtin
    """
    Locked context manager for open
    """

    return locked_opener(__builtin__.open, *args, **kwargs)

def gopen(*args, **kwargs):
    """
    Locked context manager for gzip.open
    """

    return locked_opener(gzip.open, *args, **kwargs)

def copen(*args, **kwargs):
    """
    Locked context manager for codecs.open
    """

    return locked_opener(codecs.open, *args, **kwargs)

