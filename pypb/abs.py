"""
Misc abstract classes and functions.
"""

from __future__ import division

__author__  = "Parantapa Bhattacharya <pb@parantapa.net>"

import abc
from functools import wraps

class Close(object):
    """
    Runs close() on context exiting and garbage collection.

    If close() method should be run only once make sure to use the `runonce`
    decorator. This class doesn't check if close has previously been run.
    """

    __metaclass__ = abc.ABCMeta

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @abc.abstractmethod
    def close(self):
        """
        To be overridden in the subclass.
        """

        return

def runonce(origfn):
    """
    Run the original _METHOD_ once only.
    """

    @wraps(origfn)
    def newfn(self, *args, **kwargs):
        """
        Replacement _METHOD_.
        """

        try:
            if self._closed:
                return
        except AttributeError:
            self._closed = True
            return origfn(self, *args, **kwargs)

    return newfn

