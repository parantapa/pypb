# encoding: utf-8
"""
Simple file based lock.

NOTE: Wont work on Non-POSIX systems
"""

import fcntl
from contextlib import contextmanager

@contextmanager
def flock(fname, lock_type="exclusive", blocking=True):
    """
    Make a file lock.
    """

    if lock_type == "exclusive":
        operation = fcntl.LOCK_EX
    elif lock_type == "shared":
        operation = fcntl.LOCK_SH
    else:
        raise ValueError("Invalid lock_type: '%s'" % lock_type)

    if not blocking:
        operation = operation | fcntl.LOCK_NB

    with open(fname, "ab") as fobj:
        fd = fobj.fileno()
        fcntl.lockf(fd, operation)

        yield None

        fcntl.lockf(fd, fcntl.LOCK_UN)
