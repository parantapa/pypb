"""
Simple code to check if piece of text is in English
"""

from __future__ import division, print_function

__author__ = "Parantapa Bhattachara <pb [at] parantapa [dot] net>"

import gzip
import codecs

class Dict(object):
    """
    Simple python class representing dictionary.
    """

    def __init__(self, fname, lang_perc=0.6):
        """
        Load the english words from dictionary.
        """

        with gzip.open(fname, "r") as fobj:
            fobj = codecs.getreader("latin1")(fobj)
            words = fobj.read().lower().split()

        words = set(w for w in words if w.isalpha() and len(w) > 1)

        self.words = words
        self.lang_perc = lang_perc

    def is_lang(self, text):
        """
        Check if text is in given lang.
        """

        # Lowercase and strip
        ws = text.lower().strip()

        # Remove any simple urls
        ws = ws.split()
        xws = []
        for w in ws:
            if w.startswith("http://"): continue
            if w.startswith("https://"): continue
            if w.startswith("t.co/"): continue
            xws.append(w)
        ws = " ".join(xws)

        # Replace anything non alphabetic with space
        ws = "".join(w if w.isalpha() else " " for w in ws)

        # Strip and split to get the tokens
        ws = ws.strip().split()
        ws = set(w for w in ws if len(w) > 1)
        if not ws:
            return False

        enws = ws & self.words
        test = len(enws) / len(ws) > self.lang_perc

        return test

