"""
Simple interface to python multiprocessing.
"""

from __future__ import division

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"

import time
import multiprocessing as mp
from itertools import islice

import pypb.abs

class ProcessFarm(pypb.abs.Close):
    """
    A simple process farm with used processes discarded.

    Create new processes when needed using *spawn*. Number of processes that
    may exist simultaniously is controlled by the constructor parameter
    *max_workers*
    """

    def __init__(self, max_workers=None):
        self.procs = dict()

        if max_workers is not None:
            self.max_workers = int(max_workers)
        else:
            self.max_workers = mp.cpu_count()

    @pypb.abs.runonce
    def close(self):
        self.join_all()

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a new process.

        func     - The func to be run in the new process.
        *args    - The positional arguments for _func_
        **kwargs - The keyword arguments for _func_
        """

        # Join any finished processes
        while len(self.procs) > self.max_workers:
            self.join_finished()
            time.sleep(1)

        proc = mp.Process(target=func, args=args, kwargs=kwargs)
        proc.start()

        # Add the process to the list of unjoined processes
        self.procs[proc.pid] = proc

    def join_finished(self):
        """
        Join any finished child process.
        """

        for proc in self.procs.values():
            if not proc.is_alive():
                proc.join()
                del self.procs[proc.pid]

    def join_all(self):
        """
        Join all unjoined child processes.
        """

        for proc in self.procs.values():
            proc.join()
            del self.procs[proc.pid]

    def term_all(self):
        """
        Kill any processes still running.
        """

        for p in self.procs.values():
            p.terminate()

class ImapEndMarker(object):
    """
    A special class which marks end of stream.
    """

    pass

def imap_worker(func, qin, qout):
    """
    Fetch job from queue, run it and return back.
    """

    while True:
        inp = qin.get()
        if isinstance(inp, ImapEndMarker):
            break
        out = func(inp)
        qout.put(out)

def imap(func, iterable, qsize=100, nprocs=None):
    """
    Multiprocess imap function.

    Uses multiple processes to speed up function evaluation.
    """

    iterable = iter(iterable)

    # Create the data queues
    qin     = mp.Queue(maxsize=qsize)
    qout    = mp.Queue(maxsize=qsize)
    pending = 0

    # Create a process farm
    farm = ProcessFarm(nprocs)

    try:
        # Create requisite number of processes
        for _ in range(farm.max_workers):
            farm.spawn(imap_worker, func, qin, qout)

        # Init the queues with inital data
        for inp in islice(iterable, qsize):
            qin.put(inp)
            pending += 1

        # Running phase
        for inp in iterable:
            out = qout.get()
            qin.put(inp)
            yield out

        # Finish phase
        while pending:
            out = qout.get()
            qin.put(ImapEndMarker())
            pending -= 1
            yield out
    finally:
        farm.term_all()
        farm.join_all()

