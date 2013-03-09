"""
A ZeroMQ socket wrapper implementing the MW4S pattern.

MW4S - Muliple Worker, Single Source, Single Sink
"""

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"

import time
import zmq

import pypb.abs

# Buffer time letting the sink request socket at source to close
CLOSESYNC = 15

class Error(ValueError):
    """
    Protocol error.
    """

class Message(object):
    """
    Protocol message.
    """

class WorkerJoinMessage(Message):
    """
    Worker join message.
    """

class WorkerExitedMessage(Message):
    """
    Worker exited message.
    """

class NoMoreJobsMessage(Message):
    """
    No more jobs message.
    """

class SourceExitedMessage(Message):
    """
    Source exited message.
    """

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

class Source(pypb.abs.Close):
    """
    Source(xaddrs) - job source socket

    xaddrs - tuple containing source and sink address
    """

    def __init__(self, xaddrs):
        saddr, taddr = xaddrs

        self.saddr    = saddr
        self.taddr    = taddr
        self.ssock    = rep_socket(saddr)
        self.tsock    = req_socket(taddr)
        self.nworkers = 0

    def send(self, task):
        """
        Send a task to a worker.
        """

        while True:
            req = self.ssock.recv_pyobj()

            if req is None:
                self.ssock.send_pyobj(task)
                return
            if isinstance(req, WorkerJoinMessage):
                self.ssock.send_pyobj(None)
                self.nworkers += 1
                continue
            if isinstance(req, WorkerExitedMessage):
                self.ssock.send_pyobj(None)
                self.nworkers -= 1
                continue

            raise Error("Invalid request {!r}".format(req))

    @pypb.abs.runonce
    def close(self):
        """
        Wait for all workers to finish and send close signal to sink.
        """

        while self.nworkers > 0:
            req = self.ssock.recv_pyobj()

            if req is None:
                self.ssock.send_pyobj(NoMoreJobsMessage())
                continue
            if isinstance(req, WorkerJoinMessage):
                self.ssock.send_pyobj(None)
                self.nworkers += 1
                continue
            if isinstance(req, WorkerExitedMessage):
                self.ssock.send_pyobj(None)
                self.nworkers -= 1
                continue

            raise Error("Invalid request {!r}".format(req))

        # Inform sink that source has exited
        self.tsock.send_pyobj(SourceExitedMessage())
        rep = self.tsock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

        # Close the sockets
        self.tsock.close()
        self.ssock.close()

class Sink(pypb.abs.Close):
    """
    Sink(xaddrs) -- job sink socket

    xaddrs - tuple containing source and sink address
    """

    def __init__(self, xaddrs):
        _, taddr = xaddrs

        self.taddr         = taddr
        self.tsock         = rep_socket(taddr)
        self.nworkers      = 0
        self.source_closed = False

    def __iter__(self):
        return self

    def next(self):
        """
        Return next result from worker.
        """

        while not self.source_closed or self.nworkers > 0:
            result = self.tsock.recv_pyobj()
            self.tsock.send_pyobj(None)

            if isinstance(result, WorkerJoinMessage):
                self.nworkers += 1
                continue
            if isinstance(result, WorkerExitedMessage):
                self.nworkers -= 1
                continue
            if isinstance(result, SourceExitedMessage):
                self.source_closed = True
                continue

            return result

        raise StopIteration()

    @pypb.abs.runonce
    def close(self):
        """
        Close the sockets.
        """

        # FIXME: Give the request sockets some time to close, othewise they
        # go into a infinite loop trying to reconnect.
        time.sleep(CLOSESYNC)
        self.tsock.close()

class Worker(pypb.abs.Close):
    """
    Worker(xaddrs) -- job worker socket

    xaddrs - tuple containing source and sink address
    """

    def __init__(self, xaddrs):
        saddr, taddr = xaddrs

        self.saddr  = saddr
        self.taddr  = taddr
        self.ssock  = req_socket(saddr)
        self.tsock  = req_socket(taddr)

        # Send join message to source
        self.ssock.send_pyobj(WorkerJoinMessage())
        rep = self.ssock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

        # Sent join message to sink
        self.tsock.send_pyobj(WorkerJoinMessage())
        rep = self.tsock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

    def __iter__(self):
        return self

    def next(self):
        """
        Return next task from source.
        """

        self.ssock.send_pyobj(None)
        task = self.ssock.recv_pyobj()

        if isinstance(task, NoMoreJobsMessage):
            raise StopIteration()

        return task

    def send(self, result):
        """
        Send result to sink.
        """

        self.tsock.send_pyobj(result)
        rep = self.tsock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

    @pypb.abs.runonce
    def close(self):
        """
        Inform source of leaving.
        """

        # Send exit message to source
        self.ssock.send_pyobj(WorkerExitedMessage())
        rep = self.ssock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

        # Send exit message to sink
        self.tsock.send_pyobj(WorkerExitedMessage())
        rep = self.tsock.recv_pyobj()
        if rep is not None:
            raise Error("Invalid response {!r}".format(rep))

