"""
Find process's current memory usage.

All results returned are in bytes.
Recipe copied from http://code.activestate.com/recipes/286222-memory-usage/
"""

__all__    = ["vm", "rss", "max_vm", "max_rss",
              "io_read", "io_write",
              "disk_io_read", "disk_io_write",
              "vol_ctxt_switches", "nonvol_ctxt_switches",
              "print_stats"]

import sys
from os import getpid
from datetime import datetime

SCALE = {"kB": 1024.0, "mB": 1024.0 * 1024.0,
         "KB": 1024.0, "MB": 1024.0 * 1024.0}

MEGA = 2 ** 20

# Note start time
START = datetime.utcnow()

def get_vm_data(VmKey):
    """
    Read the proc file and get virtual mem usage data.
    """

    fname = "/proc/{0}/status".format(getpid())
    
    try:
        fobj = open(fname)
    except IOError:
        # non-Linux ?
        return 0.0

    with fobj:
        for line in fobj:
            if line.startswith(VmKey):
                try:
                    _, w1, w2 = line.split()
                except ValueError:
                    # Unknown format ?
                    return 0.0

                return float(w1) * SCALE[w2]
        # Unknown key ?
        return 0.0

def vm():
    """
    Virtual memory size.
    """

    return get_vm_data("VmSize:")

def rss():
    """
    Resident set size.
    """

    return get_vm_data("VmRSS:")

def max_vm():
    """
    Max virtual memory usage.
    """

    return get_vm_data("VmPeak:")

def max_rss():
    """
    Max resident set size.
    """

    return get_vm_data("VmHWM:")

def read_proc_counter(fname, key):
    """
    Read a single value from a proc file.
    """

    try:
        fobj = open(fname)
    except IOError:
        # non-Linux ?
        return 0

    with fobj:
        for line in fobj:
            if line.startswith(key):
                try:
                    _, w = line.split()
                except ValueError:
                    # Unknown format ?
                    return 0

                return int(w)
        # Unknown key ?
        return 0

def vol_ctxt_switches():
    """
    Get the number of voluntary context switches.
    """

    fname = "/proc/{0}/status".format(getpid())
    key   = "voluntary_ctxt_switches"
    return read_proc_counter(fname, key)

def nonvol_ctxt_switches():
    """
    Get the number of involuntary context switches.
    """

    fname = "/proc/{0}/status".format(getpid())
    key   = "nonvoluntary_ctxt_switches"
    return read_proc_counter(fname, key)

def io_read():
    """
    Get the number of bytes read.
    """

    fname = "/proc/{0}/io".format(getpid())
    key   = "rchar"
    return read_proc_counter(fname, key)

def io_write():
    """
    Get the number of bytes written.
    """

    fname = "/proc/{0}/io".format(getpid())
    key   = "wchar"
    return read_proc_counter(fname, key)

def disk_io_read():
    """
    Get the number of bytes actually read from storage.
    """

    fname = "/proc/{0}/io".format(getpid())
    key   = "read_bytes"
    return read_proc_counter(fname, key)

def disk_io_write():
    """
    Get the number of bytes actually sent to to storage for writing.
    """

    fname = "/proc/{0}/io".format(getpid())
    key   = "write_bytes"
    return read_proc_counter(fname, key)

def print_stats():
    """
    Print runtime and memory usage.
    """

    now = datetime.utcnow()

    rt          = now - START
    max_vm_m     = max_vm() / MEGA
    max_rss_m     = max_rss() / MEGA
    io_read_m     = io_read() / MEGA
    io_write_m    = io_write() / MEGA
    dio_read_m    = disk_io_read() / MEGA
    dio_write_m   = disk_io_write() / MEGA
    vol_switch  = vol_ctxt_switches()
    nvol_switch = nonvol_ctxt_switches()

    print "\n"
    print "Total running time             : {}".format(rt)
    print "Peak virtual memory size       : {:.2f} MiB".format(max_vm_m)
    print "Peak resident set size         : {:.2f} MiB".format(max_rss_m)
    print "Total IO Read                  : {:.2f} MiB".format(io_read_m)
    print "Total IO Write                 : {:.2f} MiB".format(io_write_m)
    print "Disk IO Read                   : {:.2f} MiB".format(dio_read_m)
    print "Disk IO Write                  : {:.2f} MiB".format(dio_write_m)
    print "# Voluntary context switch     : {:,d}".format(vol_switch)
    print "# Non-Voluntary context switch : {:,d}".format(nvol_switch)

    sys.stdout.flush()
