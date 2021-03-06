"""
Simple wrapper over python-daemon.
"""

from __future__ import division

import os
import sys
import atexit
import tempfile
import signal
from datetime import datetime

import daemon

from pypb import exit_signal, STD_EXIT_SIGNALS
from pypb import fnamechar, abspath
from pypb.pstat import print_stats

# Constants
LOGTIMEFMT = "%Y-%m-%dT%H:%M:%S."

def daemonize(prefix=None, logdir="~/pypb_dmnlog"):
    """
    Daemonize the process.
    """

    logdir = abspath(logdir)

    # Create the directory if not exists
    if not os.path.exists(logdir):
        print "Folder '{}' doesn't exist. Creating ...".format(logdir)
        os.makedirs(logdir)

    # Default prefix is script name - the py prefix
    if prefix is None:
        prefix = sys.argv[0]
        if prefix.endswith(".py"):
            prefix = prefix[:-2]
        # Clean the prefix as it is uses in a filename
        prefix = "".join(c if fnamechar(c) else "_" for c in prefix)
    if prefix[-1] != ".":
        prefix = prefix + "."

    # Add start time to file prefix
    prefix = prefix + datetime.utcnow().strftime(LOGTIMEFMT)

    # Setup context
    dc = daemon.DaemonContext()
    dc.working_directory = "."
    dc.umask = 0o022
    dc.signal_map = dict.fromkeys(STD_EXIT_SIGNALS, exit_signal)

    # Do the redirection
    fobj = tempfile.NamedTemporaryFile(dir=logdir, delete=False,
                                       prefix=prefix, suffix=".log")
    dc.stdout = fobj
    dc.stderr = fobj

    # Print outfile name to follow
    print "STDOUT:", fobj.name
    sys.stdout.flush()

    # Ignore SIGHUP before daemonizeing
    # Otherwise the child might get a SIGHUP when daemonizing
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # Daemonize
    dc.open()

    # Register the print stats function in daemon
    atexit.register(print_stats)
