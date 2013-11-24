"""
Utility functions needed at multiple places.
"""

from __future__ import division, print_function

import os
import sys
import signal

from datetime import datetime
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

class LoopCounter(object):
    """
    Count loop iterations.
    """

    def __init__(self, step=1, maxloop=-1, logfn=log.info):
        self.start = datetime.utcnow()
        self.step = int(step)
        self.counter = 1
        self.maxloop = int(maxloop)
        self.logfn = logfn

    def eta(self):
        """
        Return expected time to finish loop.
        """

        if self.maxloop > self.counter:
            ret = datetime.utcnow() - self.start
            ret = ret // self.counter
            ret = ret * (self.maxloop - self.counter)
            return ret

        return "Unknown"

    def log(self, msg, *args, **kwargs):
        """
        Log on the step-th time.
        """

        if self.counter % self.step == 0:
            kwargs["count"] = self.counter
            kwargs["eta"] = self.eta()
            self.logfn(msg, *args, **kwargs)

        self.counter += 1

def iter_counter(iterable, msg, step=1, maxloop=-1, logfn=print):
    """
    Count loop iterations when in iterating over an iterable.
    """

    iterable = iter(iterable)

    counter = LoopCounter(step, maxloop, logfn)
    for item in iterable:
        yield item
        counter.log(msg)

