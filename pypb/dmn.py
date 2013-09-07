"""
Simple wrapper over python-daemon.
"""

from __future__ import division

import os
import sys
import signal
import atexit
import tempfile
from datetime import datetime

import daemon
import pypb.pstat as pstat

# Note start time
start = datetime.utcnow()

# Constants
LOGTIMEFMT = "%Y-%m-%d_%H:%M:%S."
MEGA = 2 ** 20

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

    rt          = end - start
    max_vm      = pstat.max_vm() / MEGA
    max_rss     = pstat.max_rss() / MEGA
    io_read     = pstat.io_read() / MEGA
    io_write    = pstat.io_write() / MEGA
    dio_read    = pstat.disk_io_read() / MEGA
    dio_write   = pstat.disk_io_write() / MEGA
    vol_switch  = pstat.vol_ctxt_switches()
    nvol_switch = pstat.nonvol_ctxt_switches()

    print "\n"
    print "Total running time             : {}".format(rt)
    print "Peak virtual memory size       : {:.2f} MiB".format(max_vm)
    print "Peak resident set size         : {:.2f} MiB".format(max_rss)
    print "Total IO Read                  : {:.2f} MiB".format(io_read)
    print "Total IO Write                 : {:.2f} MiB".format(io_write)
    print "Disk IO Read                   : {:.2f} MiB".format(dio_read)
    print "Disk IO Write                  : {:.2f} MiB".format(dio_write)
    print "# Voluntary context switch     : {:,d}".format(vol_switch)
    print "# Non-Voluntary context switch : {:,d}".format(nvol_switch)

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
