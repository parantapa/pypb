"""
Measure the progress of loops.
"""

from __future__ import division, print_function

import sys
import time as _time

DEFAULT_MSG = ("{count:,d} ({percentage:.1f} %) "
               "- elapsed: {elapsed} - eta: {eta} "
               "- {speed:.2f} loops/sec")

def td_fmt(td):
    """
    Humanize time difference in seconds.
    """

    if not td:
        return "unknown"

    td = int(td)

    mm, ss = divmod(td, 60)
    hh, mm = divmod(mm, 60)
    dd, hh = divmod(hh, 24)

    s = "%d:%02d:%02d" % (hh, mm, ss)
    if dd:
        s = "%d day%s, " % (dd, "s" if dd > 1 else "") + s
    return s

# pylint: disable=unused-variable
def make_kwargs(start, total, now, count, pnow, pcount):
    """
    Make kwargs for progress func.
    """

    td = now - pnow

    # Compute speed
    if td:
        speed = (count - pcount) / td
    else:
        speed = 0.0

    # Compute percentage
    if total:
        percentage = count / total * 100
    else:
        percentage = 0.0

    # Compute eta
    if total and speed:
        eta = (total - count) / speed
    else:
        eta = 0.0

    # Compute elapsed
    elapsed = now - start

    # Format int
    eta = td_fmt(eta)
    elapsed = td_fmt(elapsed)

    return locals()

# pylint: disable=line-too-long
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

    # Speed hack
    time = _time.time

    # Initialize
    start = time()
    count = 0
    msg = DEFAULT_MSG if msg is None else msg
    msg_fmt = msg.format

    # Setup clean
    if clean not in (True, False, None):
        raise ValueError("Invalid value for parameter 'clean'")
    if clean is None and "\n" not in msg:
        clean = sys.stdout.isatty()

    # Define the print function
    if clean:
        def myprint(newmsg, lastmsg):
            print("\r" + " " * len(lastmsg), end="")
            print("\r" + newmsg, end="")
            sys.stdout.flush()
    else:
        def myprint(newmsg, _):
            logfn(newmsg)

    # try to get the number of loops from here
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = 0

    # Go to a new line for printing
    pnow = start
    pcount = count
    lastmsg = ""

    # Yield an item
    for item in iterable:
        yield item

        # Update count and get current time
        count += 1
        now = time()

        # If enough time has passed print the progress
        if now - pnow >= mininterval:
            kwargs = make_kwargs(start, total, now, count, pnow, pcount)
            newmsg = msg_fmt(**kwargs)
            myprint(newmsg, lastmsg)

            # Update the time
            pnow = now
            pcount = count
            lastmsg = newmsg

    # Make sure to print the last output line
    now = time()

    kwargs = make_kwargs(start, total, now, count, pnow, pcount)
    newmsg = msg_fmt(**kwargs)
    myprint(newmsg, lastmsg)

def prange(*args, **kwargs):
    """
    Wrapper for progress(xrange(n))
    """

    return progress(xrange(*args), **kwargs)

def main():
    for i in prange(1000000):
        # _time.sleep(1)
        pass

if __name__ == "__main__":
    main()

