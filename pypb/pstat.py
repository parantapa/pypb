"""
Find process's current memory usage.

All results returned are in bytes.
Recipe copied from http://code.activestate.com/recipes/286222-memory-usage/
"""

__all__    = ["vm", "rss", "max_vm", "max_rss",
              "io_read", "io_write",
              "disk_io_read", "disk_io_write",
              "vol_ctxt_switches", "nonvol_ctxt_switches"]

from os import getpid

SCALE = {"kB": 1024.0, "mB": 1024.0 * 1024.0,
         "KB": 1024.0, "MB": 1024.0 * 1024.0}

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
