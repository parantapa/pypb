# encoding: utf-8
"""
Read and write data to a text file atomically.

http://stackoverflow.com/questions/2333872/atomic-writing-to-file-with-python
"""

import os
import os.path

TMP_SUFFIX = ".astore-tmp"

def make_consistant(fname):
    """
    Make the data consistant.
    """

    if os.path.exists(fname + TMP_SUFFIX):
        if os.path.exists(fname):
            os.remove(fname)
        os.rename(fname + TMP_SUFFIX, fname)

def read(fname, mode="rb"):
    """
    Slurp contents of the file.
    """

    make_consistant(fname)
    with open(fname, mode) as fobj:
        return fobj.read()

def write(text, fname, mode="wb"):
    """
    Write contents to the file.
    """

    with open(fname + TMP_SUFFIX, mode) as fobj:
        fobj.write(text)
        fobj.flush()
        os.fsync(fobj.fileno())
    make_consistant(fname)

