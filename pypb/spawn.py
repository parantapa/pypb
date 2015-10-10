"""
Simple interface to python multi-tasking.
"""

import sys
import time
import multiprocessing as mp

import gevent
import gevent.queue as gq
import setproctitle as spt

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
        self.timeout = None

    @pypb.abs.runonce
    def close(self):
        self.kill_all()
        self.join_all()

    def join_all(self, procs=None, wait=True):
        """
        Join all unjoined child processes.
        """

        if procs is None:
            local_procs = set(self.procs)
        else:
            local_procs = set(procs)

        while local_procs:
            for p in list(local_procs):
                assert p in self.procs

                if self._is_alive(p):
                    if self._has_timedout(p):
                        self._kill(p)

                if self._is_alive(p): continue

                self._join(p)
                self.procs.discard(p)
                local_procs.discard(p)

            if not wait: return
            time.sleep(1)

    def kill_all(self):
        """
        Kill any processes still running.
        """

        procs = list(self.procs)
        for proc in procs:
            self._kill(proc)

    def spawn(self, func, *args, **kwargs):
        raise NotImplementedError()

    def make_queue(self, maxsize=0):
        raise NotImplementedError()

    def _kill(self, proc):
        raise NotImplementedError()

    def _join(self, proc):
        raise NotImplementedError()

    def _is_alive(self, proc):
        raise NotImplementedError()

    def _has_timedout(self, proc):
        raise NotImplementedError()

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

    def __init__(self, max_procs=sys.maxsize, timeout=None):
        super(ProcessFarm, self).__init__()

        self.procnum = 0
        self.max_procs = max_procs
        self.timeout = timeout

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a new process.

        func       - The func to be run in the new process.
        *args      - The positional arguments for _func_
        **kwargs   - The keyword arguments for _func_
        """

        while len(self.procs) >= self.max_procs:
            self.join_all(wait=False)
            time.sleep(1.0)

        self.procnum += 1

        pargs = (self.procnum, func, args, kwargs)
        p = mp.Process(target=proc_init_run, args=pargs)
        p.start()

        proc = (time.time(), p)
        self.procs.add(proc)

        return proc

    def _kill(self, proc):
        return proc[1].terminate()

    def _join(self, proc):
        return proc[1].join()

    def _is_alive(self, proc):
        return proc[1].is_alive()

    def _has_timedout(self, proc):
        if self.timeout is None:
            return False
        return (time.time() - proc[0]) >= self.timeout

    def make_queue(self, maxsize=0):
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
        return proc.kill()

    def _join(self, proc):
        return proc.join()

    def _is_alive(self, proc):
        return not bool(proc.ready())

    def _has_timedout(self, proc):
        return False

    def make_queue(self, maxsize=0):
        return gq.Queue(maxsize)

