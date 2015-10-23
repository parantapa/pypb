"""
Simple interface to python multi-tasking.
"""

import sys
import abc
import time
import multiprocessing as mp

import gevent
import gevent.queue as gq
import setproctitle as spt

import pypb.abs

cpu_count = mp.cpu_count

class TaskFarm(pypb.abs.Close):
    """
    Base class for task farms.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.procs = set()

    @pypb.abs.runonce
    def close(self):
        self.kill_all()
        self.join_all()

    def _join_all_any(self, procs, return_on_any):
        """
        Join processes.
        """

        if procs is None:
            procs = set(self.procs)

        count_joined = 0
        while procs:
            for p in list(procs):
                assert p in self.procs

                if self._is_alive(p):
                    continue

                self._join(p)
                self.procs.discard(p)
                procs.discard(p)
                count_joined += 1

            if return_on_any and count_joined > 0:
                return
            time.sleep(1)

    def join_all(self, procs=None):
        """
        Wait till all the processes have finished.
        """

        self._join_all_any(procs, False)

    def join_any(self, procs=None):
        """
        Wait till any one of the processes finishes.
        """

        self._join_all_any(procs, True)

    def kill_all(self):
        """
        Kill any processes still running.
        """

        procs = list(self.procs)
        for proc in procs:
            self._kill(proc)

    @abc.abstractmethod
    def spawn(self, func, *args, **kwargs):
        pass

    @abc.abstractmethod
    def make_queue(self, maxsize=0):
        pass

    @abc.abstractmethod
    def _kill(self, proc):
        pass

    @abc.abstractmethod
    def _join(self, proc):
        pass

    @abc.abstractmethod
    def _is_alive(self, proc):
        pass

def proc_init_run(procnum, func, args, kwargs):
    """
    Set the process title and run.
    """

    title = spt.getproctitle()
    title = "{} : {} : {}".format(title, procnum, func.__name__)
    spt.setproctitle(title)

    return func(*args, **kwargs)

class ProcessFarm(TaskFarm):
    """
    Spawn processes.
    """

    def __init__(self, max_procs=sys.maxsize):
        super(ProcessFarm, self).__init__()

        self.procnum = 0
        self.max_procs = max_procs

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a new process.

        func       - The func to be run in the new process.
        *args      - The positional arguments for _func_
        **kwargs   - The keyword arguments for _func_
        """

        if len(self.procs) == self.max_procs:
            self.join_any()

        self.procnum += 1

        pargs = (self.procnum, func, args, kwargs)
        proc = mp.Process(target=proc_init_run, args=pargs)
        proc.start()
        self.procs.add(proc)

        return proc

    def make_queue(self, maxsize=0):
        return mp.Queue(maxsize)

    def _kill(self, proc):
        return proc.terminate()

    def _join(self, proc):
        return proc.join()

    def _is_alive(self, proc):
        return proc.is_alive()

class GreenletFarm(TaskFarm):
    """
    Spawn greenlets.
    """

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a new greenlet.

        func       - The func to be run in the new greenlets.
        *args      - The positional arguments for _func_
        **kwargs   - The keyword arguments for _func_
        """

        proc = gevent.spawn(func, *args, **kwargs)
        self.procs.add(proc)

        return proc

    def make_queue(self, maxsize=0):
        return gq.Queue(maxsize)

    def _kill(self, proc):
        return proc.kill()

    def _join(self, proc):
        return proc.join()

    def _is_alive(self, proc):
        return not bool(proc.ready())


