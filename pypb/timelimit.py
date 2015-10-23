# encoding: utf-8
"""
Functions to support timeout
"""

import signal
from contextlib import contextmanager

class TimeoutException(Exception):
    """
    Raised by the time_limit context manager.
    """

def timeout_handler(_a, _b): # pylint: disable=unused-argument
    """
    Handle the timeout signal.
    """

    raise TimeoutException()

@contextmanager
def timelimit(seconds):
    """
    Finish the function within a time limit.

    NOTE: Uses SIGALRM for this.
    """

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
