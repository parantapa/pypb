"""
Utility functions needed at multiple places.
"""

from __future__ import division, print_function

import os
import sys
import signal

from functools import wraps

# Standard exit signals not handled by Python directly
STD_EXIT_SIGNALS = [
    signal.SIGINT,
    signal.SIGQUIT,
    signal.SIGHUP,
    signal.SIGTERM
]

def exit_signal(signum, _):
    """
    Handle exit signal.
    """

    signame = "Unknown Signal"
    for v, k in signal.__dict__.iteritems():
        if v.startswith('SIG') and k == signum:
            signame = v
            break

    msg = "{} : Received signal - {} ({})\n"
    msg = msg.format(os.getpid(), signame, signum)
    sys.stderr.write(msg)
    sys.stderr.flush()

    sys.exit(0)

def register_exit_signals():
    """
    Register exit handler for the standard exit signals.
    """

    for n in STD_EXIT_SIGNALS:
        signal.signal(n, exit_signal)

def coroutine(origfn):
    """
    Generator to prime coroutines.
    """

    @wraps(origfn)
    def wrapfn(*args, **kwargs):
        """
        Prime the coroutine by calling next.
        """

        cr = origfn(*args, **kwargs)
        cr.next()
        return cr

    return wrapfn

def abspath(path):
    """
    Return the canonical path.
    """

    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    path = os.path.abspath(path)
    return path

def fnamechar(c):
    """
    Check if char is a valid filename char.
    """

    return (c.isalnum() or c in "-_.") # pylint: disable=superfluous-parens
