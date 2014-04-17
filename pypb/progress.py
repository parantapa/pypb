"""
Measure the progress of loops.
"""

from __future__ import division, print_function

import sys
from datetime import datetime, timedelta

DEFAULT_MSG = ("Count: {count:,d} Percentage: {percentage:.1f} "
               "Elapsed: {elapsed} ETA: {eta} "
               "Speed: {speed} Loops/Second")

def progress(iterable, msg=None, total=None, mininterval=1, logfn=print, clean=None):
    """
    Print progress of loop iteration.

    iterable    - Iterable to loop over.
    msg         - The msg used to display progress.
    total       - Total number of items in the iterable.
    mininterval - Minimum number of seconds to wait before updating progress
    logfn       - Function used to print the progress message
    clean       - If True re-use one line to print output; False otherwise
                  Default is 'auto'. Note: Make sure to set clean=False
                  if using a custom logfn. Otherwise it will ignore logfn
                  if stdout is a tty. This assumes that the

    The format method of the `msg' parameter is called with the following
    keyword arguments before updating the progress.

    count      : Number of items already iterated over.
    percentage : Percentage of items already iterated over.
    elapsed    : Time elapsed since beginning of loop.
    eta        : Expected time for the loop to finish.
    speed      : Number of items iterated per second.
    """


    # Initialize
    start = datetime.utcnow()
    count = 0
    mininterval = timedelta(seconds=mininterval)
    last = start
    msg = DEFAULT_MSG if msg is None else msg

    # Setup clean
    if clean not in (True, False, None):
        raise ValueError("Invalid value for parameter 'clean'")
    if clean is None and "\n" not in msg:
        clean = sys.stdout.isatty()

    # try to get the number of loops from here
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None

    if clean:
        lastmsg = ""
        print(lastmsg)


    # Yield an item
    for item in iterable:
        yield item

        # Update count and get current time
        count += 1
        now = datetime.utcnow()

        # If enough time has passed print the progress
        if now - last >= mininterval:
            kwargs = {}
            kwargs["count"] = count
            kwargs["elapsed"] = now - start
            if total is not None:
                kwargs["eta"] = ((now - start) // count) * (total - count)
                kwargs["percentage"] = count / total * 100
            else:
                kwargs["eta"] = "Unknown"
                kwargs["percentage"] = "Unknown"
            try:
                kwargs["speed"] = count / (now - start).seconds
            except ZeroDivisionError:
                kwargs["speed"] = "Unknown"
            if clean:
                print("\r" + " " * len(lastmsg), end="")
                lastmsg = "\r" + msg.format(**kwargs)
                print(lastmsg, end="")
                sys.stdout.flush()
            else:
                logfn(msg.format(**kwargs))

            # Update the time
            last = now

    # Make sure to print the last output line
    now = datetime.utcnow()
    kwargs = {}
    kwargs["count"] = count
    kwargs["elapsed"] = now - start
    if total is not None:
        kwargs["eta"] = ((now - start) // count) * (total - count)
        kwargs["percentage"] = count / total * 100
    else:
        kwargs["eta"] = "Unknown"
        kwargs["percentage"] = "Unknown"
    try:
        kwargs["speed"] = count / (now - start).seconds
    except ZeroDivisionError:
        kwargs["speed"] = "Unknown"
    if clean:
        print("\r" + " " * len(lastmsg), end="")
        lastmsg = "\r" + msg.format(**kwargs)
        print(lastmsg)
        sys.stdout.flush()
    else:
        logfn(msg.format(**kwargs))

def prange(*args, **kwargs):
    """
    Wrapper for progress(xrange(n))
    """

    return progress(xrange(*args), **kwargs)
