"""
Utility functions needed at multiple places.
"""

from __future__ import division, print_function

import os
import sys
import signal

from functools import wraps

from logbook import Logger
log = Logger(__name__)

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

    msg = "{} : Received signal - {} ({})"
    msg = msg.format(os.getpid(), signame, signum)
    print(msg)
    sys.stdout.flush()

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

