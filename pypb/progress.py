"""
Measure the progress of loops.
"""

from __future__ import division, print_function

import sys
from datetime import datetime, timedelta

DEFAULT_MSG = ("{count:,d} ({percentage:.1f} %) "
               "- elapsed: {elapsed} - eta: {eta} "
               "- {speed:.2f} loops/sec")

def timedelta_format(td):
    """
    Return a better string for timedelta object.
    """

    if isinstance(td, (str, unicode)):
        return td

    mm, ss = divmod(td.seconds, 60)
    hh, mm = divmod(mm, 60)
    s = "%d:%02d:%02d" % (hh, mm, ss)
    if td.days:
        s = "%d day%s, " % (td.days, "s" if td.days > 1 else "") + s
    return s

def make_progress_kwargs(start, now, count, total):
    """
    Make progress func kwargs.
    """

    elapsed = now - start

    if total is not None and count > 0:
        eta = (elapsed // count) * (total - count)
        percentage = count / total * 100
    else:
        eta = "unknown"
        percentage = float("nan")

    if elapsed.seconds > 0:
        speed = count / elapsed.seconds
    else:
        speed = float("nan")

    elapsed = timedelta_format(elapsed)
    eta = timedelta_format(eta)

    return locals()

def clean_print(newmsg, lastmsg):
    """
    Print a new msg by overwriting the last msg on screen.
    """

    print("\r" + " " * len(lastmsg), end="")
    print("\r" + newmsg, end="")
    sys.stdout.flush()

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
                  if stdout is a tty.

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

    # Go to a new line for printing
    lastmsg = ""

    # Yield an item
    for item in iterable:
        yield item

        # Update count and get current time
        count += 1
        now = datetime.utcnow()

        # If enough time has passed print the progress
        if now - last >= mininterval:
            kwargs = make_progress_kwargs(start, now, count, total)
            newmsg = msg.format(**kwargs)
            if clean:
                clean_print(newmsg, lastmsg)
            else:
                logfn(newmsg)

            # Update the time
            last = now
            lastmsg = newmsg

    # Make sure to print the last output line
    now = datetime.utcnow()

    kwargs = make_progress_kwargs(start, now, count, total)
    if clean:
        clean_print(newmsg, lastmsg)
        print("")
    else:
        logfn(newmsg)

def prange(*args, **kwargs):
    """
    Wrapper for progress(xrange(n))
    """

    return progress(xrange(*args), **kwargs)

def main():
    import time
    for i in prange(10):
        time.sleep(1)

if __name__ == "__main__":
    main()

