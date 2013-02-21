"""
Find process's current memory usage.

All results returned are in bytes.
Recipe copied from http://code.activestate.com/recipes/286222-memory-usage/
"""

__author__ = "Parantapa Bhattacharya <pb@parantapa.net>"
__all__    = ["vm", "rss", "max_vm", "max_rss"]

from os import getpid

SCALE = {"kB": 1024.0, "mB": 1024.0 * 1024.0,
         "KB": 1024.0, "MB": 1024.0 * 1024.0}

def VmB(VmKey):
    """
    Read the proc file and get the data.
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

    return VmB("VmSize:")

def rss():
    """
    Resident set size.
    """

    return VmB("VmRSS:")

def max_vm():
    """
    Max virtual memory usage.
    """

    return VmB("VmPeak:")

def max_rss():
    """
    Max resident set size.
    """

    return VmB("VmHWM:")

