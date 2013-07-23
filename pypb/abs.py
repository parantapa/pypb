"""
Misc abstract classes and functions.
"""

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

    attrname = "_%s_once_result" % id(origfn)

    @wraps(origfn)
    def newfn(self, *args, **kwargs):
        """
        Replacement _METHOD_.
        """

        try:
            return getattr(self, attrname)
        except AttributeError:
            setattr(self, attrname, origfn(self, *args, **kwargs))
            return getattr(self, attrname)

    return newfn

