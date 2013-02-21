"""
A ZeroMQ socket wrapper implementing the MW4S pattern.

MW4S - Muliple Worker, Single Source, Single Sink
"""

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"

import time

import zmq

# Sync time before closing
CLOSESYNC = 15

class Error(ValueError):
    """
    M4WS protocol error
    """

    pass

def rep_socket(addr):
    """
    Create a reply socket and bind it.
    """

    context = zmq.Context.instance()
    sock = context.socket(zmq.REP)
    sock.bind(addr)

    return sock

def req_socket(addr):
    """
    Create a request socket and connect it.
    """

    context = zmq.Context.instance()
    sock = context.socket(zmq.REQ)
    sock.connect(addr)

    return sock

class Source(object):
    """
    Source(saddr, taddr) - job source socket

    saddr - source address
    taddr - sink address
    """

    def __init__(self, saddr, taddr):
        self.saddr = saddr
        self.taddr = taddr
        self.ssock = rep_socket(saddr)
        self.tsock = req_socket(taddr)
        self.nworkers = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def send(self, task):
        """
        Send a task to a worker.
        """

        while True:
            req = self.ssock.recv_pyobj()
            if req is None:
                self.ssock.send_pyobj(task)
                return

            if isinstance(req, bool):
                self.ssock.send_pyobj(None)
                self.nworkers += (1 if req else -1)
                continue

            raise Error("Invalid request {!r}".format(req))

    def close(self):
        """
        Wait for all workers to finish and send close signal to sink.
        """

        while self.nworkers > 0:
            req = self.ssock.recv_pyobj()
            if req is None:
                self.ssock.send_pyobj(StopIteration())
                continue

            if isinstance(req, bool):
                self.ssock.send_pyobj(None)
                self.nworkers += (1 if req else -1)
                continue

            raise Error("Invalid request {!r}".format(req))

        self.tsock.send_pyobj(StopIteration())
        rep = self.tsock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

        # FIXME: Time the closing between source and sink.
        # This hack is to make sure the race condition, when the bound socket
        # is closed before the connecting socket doesn't happen. Otherwise
        # ZeroMQ goes into a loop trying to reconnect to the remote side.
        # Currently I don't know a better way to fix this.
        self.tsock.close()
        time.sleep(CLOSESYNC)
        self.ssock.close()

class Sink(object):
    """
    Sink(saddr, taddr) -- job sink socket

    saddr - source address
    taddr - sink address
    """

    def __init__(self, saddr, taddr):
        self.saddr = saddr
        self.taddr = taddr
        self.ssock = req_socket(saddr)
        self.tsock = rep_socket(taddr)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def next(self):
        """
        Return next result from worker.
        """

        result = self.tsock.recv_pyobj()
        self.tsock.send_pyobj(None)

        if isinstance(result, StopIteration):
            raise result

        return result

    def close(self):
        """
        Close the sockets.
        """

        # FIXME: Time the closing between source and sink.
        # See the note for SourceSocket.close.
        self.ssock.close()
        time.sleep(CLOSESYNC)
        self.tsock.close()

class Worker(object):
    """
    Worker(saddr, taddr) -- job worker socket

    saddr - source address
    taddr - sink address
    """

    def __init__(self, saddr, taddr):
        self.saddr = saddr
        self.taddr = taddr
        self.ssock = req_socket(saddr)
        self.tsock = req_socket(taddr)

        self.ssock.send_pyobj(True)
        rep = self.ssock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def next(self):
        """
        Return next task from source.
        """

        self.ssock.send_pyobj(None)
        task = self.ssock.recv_pyobj()

        if isinstance(task, StopIteration):
            raise task

        return task

    def send(self, result):
        """
        Send result to sink.
        """

        self.tsock.send_pyobj(result)
        rep = self.tsock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

    def close(self):
        """
        Inform source of leaving.
        """

        self.ssock.send_pyobj(False)
        rep = self.ssock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

