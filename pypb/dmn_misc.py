"""
Simple helpers for createing nohup daemon processes
"""

from __future__ import division, print_function

import os
import os.path

def dmn_pid(fname):
    """
    Return the pid of process from given file.
    """

    if os.path.exists(fname):
        with open(fname, "r") as fobj:
            pid = fobj.read().strip()
            try:
                pid = int(pid)
            except ValueError:
                return None

            try:
                os.kill(pid, 0)
                return pid
            except OSError:
                return None

    return None

def dmn_cmd(cmd, pfname, ofname="/dev/null"):
    """
    Wrap the given command in nohup and save pid to file.
    """

    fmt = "nohup {} </dev/null >>{} 2>&1 & echo $! > {}"
    return fmt.format(cmd, ofname, pfname)
