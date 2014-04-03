"""
Measure the progress of loops.
"""

from __future__ import division, print_function

from datetime import datetime, timedelta

DEFAULT_MSG = ("Count: {count:,d} Percentage: {percentage:.1f} "
               "Elapsed: {elapsed} ETA: {eta} "
               "Speed: {speed} Loops/Second")

def progress(iterable, msg=None, total=None, mininterval=1, logfn=print):
    """
    Print progress of loop iteration.

    iterable    - Iterable to loop over
    msg         - The msg used to display progress.
    total       - Total number of items in the iterable.
    mininterval - Minimum number of seconds to wait before updating progress/
    logfn       - Function used to print the progress message/

    The format method of the `msg' parameter is called with the following
    keyword arguments before updating the progress.

    count      : Number of items already iterated over
    percentage : Percentage of items already iterated over
    elapsed    : Time elapsed since beginning of loop
    eta        : Expected time for the loop to finish
    speed      : Number of items iterated per second
    """

    # Initialize
    start = datetime.utcnow()
    count = 0
    mininterval = timedelta(seconds=mininterval)
    last = start
    msg = DEFAULT_MSG if msg is None else msg

    # try to get the number of loops from here
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None

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
            kwargs["speed"] = count / (now - start).seconds
            logfn(msg.format(**kwargs))

            # Update the time
            last = now

def prange(*args, **kwargs):
    """
    Wrapper for progress(xrange(n))
    """

    return progress(xrange(*args), **kwargs)
