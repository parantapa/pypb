"""
Simple interface to python multiprocessing.
"""

from __future__ import division

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"

import time
import atexit
import multiprocessing as mp

from logbook import Logger
log = Logger(__name__)

class ProcessFarm(object):
    """
    A simple process farm new processes are generated on spawn.
    """

    def __init__(self, max_workers=None):
        self.procs = dict()

        if max_workers is not None:
            self.max_workers = int(max_workers)
        else:
            self.max_workers = mp.cpu_count()

        # Register joinall to run at exit
        atexit.register(self.joinall)

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a new process.

        func     - The func to be run in the new process.
        *args    - The positional arguments for _func_
        **kwargs - The keyword arguments for _func_
        """

        # Join any finished processes
        while len(self.procs) > self.max_workers:
            self.joinall(False)
            time.sleep(1)

        proc = mp.Process(target=func, args=args, kwargs=kwargs)
        proc.start()

        # Add the process to the list of unjoined processes
        self.procs[proc.pid] = proc

    def joinall(self, wait=True):
        """
        Join all unjoined processes.

        wait - Wait for process to finish.
        """

        for proc in self.procs.values():
            if wait:
                proc.join()
                del self.procs[proc.pid]
            else:
                if not proc.is_alive():
                    proc.join()
                    del self.procs[proc.pid]

    def termall(self):
        """
        Kill any processes still running.
        """

        for p in self.procs.values():
            p.terminate()

