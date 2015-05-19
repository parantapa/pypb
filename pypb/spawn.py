"""
Simple interface to python multi-tasking.
"""

import multiprocessing as mp

import gevent
import gevent.queue as gq

import pypb.abs

def cpu_count():
    """
    Return the CPU count.
    """

    return mp.cpu_count()

class TaskFarm(pypb.abs.Close):
    """
    Base class for task farms.
    """

    def __init__(self):
        self.procs = set()

    @pypb.abs.runonce
    def close(self):
        self.kill_all()
        self.join_all()

    def join_all(self, procs=None):
        """
        Join all unjoined child processes.
        """

        if procs is None:
            procs = list(self.procs)

        for proc in procs:
            if proc in self.procs:
                self.join(proc)
                self.procs.discard(proc)

    def kill_all(self):
        """
        Kill any processes still running.
        """

        procs = list(self.procs)
        for proc in procs:
            self.kill(proc)

    def spawn(self, func, *args, **kwargs):
        raise NotImplementedError()

    def make_queue(self, maxsize=0):
        raise NotImplementedError()

    def _kill(self, proc):
        raise NotImplementedError()

    def _join(self, proc):
        raise NotImplementedError()

class ProcessFarm(TaskFarm):
    """
    Spawn processes.
    """

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a new process.

        func       - The func to be run in the new process.
        *args      - The positional arguments for _func_
        **kwargs   - The keyword arguments for _func_
        """

        proc = mp.Process(target=func, args=args, kwargs=kwargs)
        proc.start()
        self.procs.add(proc)

        return proc

    def _kill(self, proc):
        """
        Kill the given process.
        """

        return proc.terminate()

    def _join(self, proc):
        """
        Join the process.
        """

        return proc.join()

    def make_queue(self, maxsize=0):
        """
        Create a queue.
        """

        return mp.Queue(maxsize)

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

    def _kill(self, proc):
        """
        Kill the greenlet.
        """

        return proc.kill()

    def _join(self, proc):
        """
        Join the greenlet.
        """

        return proc.join()

    def make_queue(self, maxsize=0):
        """
        Create a queue.
        """

        return gq.Queue(maxsize)

