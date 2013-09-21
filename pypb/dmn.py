"""
Simple wrapper over python-daemon.
"""

from __future__ import division

import os
import sys
import atexit
import tempfile
from datetime import datetime

import daemon

from pypb import exit_signal, STD_EXIT_SIGNALS
from pypb.pstat import print_stats

# Constants
LOGTIMEFMT = "%Y-%m-%d_%H:%M:%S."

# Cache directory to store results
LOGDIR = "/var/tmp/{}/pypb/log/".format(os.environ["LOGNAME"])

def daemonize(prefix=None):
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
    dc.signal_map = dict.fromkeys(STD_EXIT_SIGNALS, exit_signal)

    # Create the directory if not exists
    if not os.path.exists(LOGDIR):
        print "Folder '{}' doesn't exist. Creating ...".format(LOGDIR)
        os.makedirs(LOGDIR)

    # Do the redirection
    fobj = tempfile.NamedTemporaryFile(dir=LOGDIR, delete=False,
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
