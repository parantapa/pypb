"""
Utility functions needed at multiple places.
"""

from __future__ import division

__author__ = "Parantapa Bhattacharya <pb@parantapa.net>"

import os
import sys
import signal
import atexit
import tempfile
from datetime import datetime
from functools import wraps

import daemon
import pypb.pstat as pstat

LOGTIMEFMT = "%Y-%m-%d_%H:%M:%S."

# Define logger
from logbook import Logger
log = Logger(__name__)

# Note start time
start = datetime.utcnow()

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
    print msg
    sys.stdout.flush()

    sys.exit(0)

def print_stats():
    """
    Print runtime and memory usage.
    """

    # Note end time
    end = datetime.utcnow()

    rt      = end - start
    max_vm  = pstat.max_vm() / (2 ** 20)
    max_rss = pstat.max_rss() / (2 ** 20)

    print "Total running time       : {}".format(rt)
    print "Peak virtual memory size : {:.2f} MiB".format(max_vm)
    print "Peak resident set size   : {:.2f} MiB".format(max_rss)
    sys.stdout.flush()

def daemonize(logdir, prefix=None):
    """
    Daemonize the process.
    """

    # Default prefix is script name - the py prefix
    if prefix is None:
        prefix = sys.argv[0]
        if prefix.endswith(".py"):
            prefix = prefix[:-2]
    if prefix[-1] != ".":
        prefix = prefix + "."

    # Add start time to file prefix
    prefix = prefix + datetime.utcnow().strftime(LOGTIMEFMT)

    # Setup context
    dc = daemon.DaemonContext()
    dc.working_directory = "."
    dc.umask = 0o022
    dc.signal_map = {
        signal.SIGINT:  exit_signal,
        signal.SIGQUIT: exit_signal,
        signal.SIGHUP:  exit_signal,
        signal.SIGTERM: exit_signal
    }

    # Create the directory if not exists
    if not os.path.exists(logdir):
        print "Folder '{}' doesn't exist. Creating ...".format(logdir)
        os.makedirs(logdir)
    
    # Do the redirection
    fobj = tempfile.NamedTemporaryFile(dir=logdir, delete=False,
                                       prefix=prefix, suffix=".log")
    dc.stdout = fobj
    dc.stderr = fobj

    # Print outfile name to follow
    print "STDOUT:", fobj.name
    sys.stdout.flush()

    # Daemonize
    dc.open()

    # Register the print stats function in daemon
    atexit.register(print_stats)

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

