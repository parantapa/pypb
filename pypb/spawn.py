"""
Simple interface to python multiprocessing.
"""

import time
import multiprocessing as mp

import pypb.abs

# Add multiprocessing.Queue in namespace
Queue = mp.Queue

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
        self.term_all()
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

        return proc

    def join_finished(self):
        """
        Join any finished child process.
        """

        for proc in self.procs.values():
            if not proc.is_alive():
                proc.join()
                del self.procs[proc.pid]

    def join_all(self, procs=None):
        """
        Join all unjoined child processes.
        """

        if procs is None:
            procs = self.procs.values()

        for proc in procs:
            proc.join()
            if proc.pid in self.procs:
                del self.procs[proc.pid]

    def term_all(self):
        """
        Kill any processes still running.
        """

        for p in self.procs.values():
            p.terminate()
