#!/usr/bin/env python2
# encoding: utf-8
"""
Usage:
  ./zjson.py encode [-m <CodeKeyFile>] [-d] [<DecodedFile> [<EncodedFile>]]
  ./zjson.py decode [-m <CodeKeyFile>] [<EncodedFile> [<DecodedFile>]]
  ./zjson.py mkdict [<CorpusFile> [<CodeKeyFile>]]


Create compressed json objects.
"""

from __future__ import division, print_function

__author__ = "Parantapa Bhattachara <pb [at] parantapa [dot] net>"

import sys
import gzip
import json
from collections import Counter
from contextlib import contextmanager

from json import dumps as _dumps
from json import loads as _loads

from zlib import compress as _compress
from zlib import decompress as _decompress

class ObjectMinifier(object):
    """
    Create minified version of json.
    """

    def __init__(self, code_key_fname):
        with open(code_key_fname) as fobj:
            code_keys = json.load(fobj)

        self.key_code = {k: c for c, k in code_keys}
        self.code_key = {c: k for c, k in code_keys}

    def minify(self, obj, drop_empty):
        """
        Minify the object.
        """

        if isinstance(obj, dict):
            ret = {}
            for k, v in obj.iteritems():
                v = self.minify(v, drop_empty)
                if drop_empty and v is None:
                    continue
                k = self.key_code[k]
                ret[k] = v
            return ret

        if isinstance(obj, (list, tuple)):
            if drop_empty and not obj:
                return None
            else:
                return [self.minify(v, drop_empty) for v in obj]

        return obj

    def unminify(self, obj):
        """
        Unminify the object.
        """

        if isinstance(obj, dict):
            return {
                self.code_key[k]: self.unminify(v)
                for k, v in obj.iteritems()
            }

        if isinstance(obj, (list, tuple)):
            return [self.unminify(v) for v in obj]

        return obj

_MINIFIER = None

def set_minifier(fname):
    """
    Load the global minifier.
    """

    global _MINIFIER # pylint: disable=global-statement

    _MINIFIER = ObjectMinifier(fname)

def minify(obj, drop_empty):
    """
    Minify the object.
    """

    if _MINIFIER is None:
        raise RuntimeError("Minifier not set")

    return _MINIFIER.minify(obj, drop_empty)

def unminify(obj):
    """
    Unminify the object.
    """

    if _MINIFIER is None:
        raise RuntimeError("Minifier not set.")

    return _MINIFIER.unminify(obj)

def dumps(obj, do_minify=True, do_compres=True, drop_empty=True):
    """
    Return an encoded version of the given object.
    """

    if do_minify:
        obj = minify(obj, drop_empty)
    obj = _dumps(obj, separators=(',',':'))
    if do_compres:
        obj = _compress(obj)

    return obj

def loads(obj, do_unminify=True, do_decompres=True):
    """
    Return the decoded version of the given object.
    """

    if do_decompres:
        obj = _decompress(obj)
    obj = _loads(obj)
    if do_unminify:
        obj = unminify(obj)

    return obj

def short_code(number, alphabet='abcdefghijklmnopqrstuvwxyz'):
    """
    Converts an integer to a alphabetic string.
    """

    if not isinstance(number, (int, long)):
        raise TypeError('Input must be an integer.')

    if number < 0:
        raise ValueError("Input must be postive.")

    if 0 <= number < len(alphabet):
        return alphabet[number]

    code = ''
    while number != 0:
        number, i = divmod(number, len(alphabet))
        code = alphabet[i] + code

    return code

def get_keys(obj):
    """
    Get a set of keys from the tweet.
    """

    keys = set()
    objstack = [obj]

    while objstack:
        obj = objstack.pop()

        if isinstance(obj, dict):
            keys.update(obj.iterkeys())
            objstack.extend(obj.itervalues())

        elif isinstance(obj, (list, tuple)):
            objstack.extend(obj)

    return keys

@contextmanager
def do_open(fname, mode):
    """
    Open the file.
    """

    if fname is None or fname == "-":
        if "r" in mode:
            yield sys.stdin
        else:
            yield sys.stdout
            sys.stdout.flush()
        return

    if fname.endswith(".gz"):
        fobj = gzip.open(fname, mode)
    else:
        fobj = open(fname, mode)
    yield fobj
    fobj.close()

def make_codekey_file(ifname, ofname):
    """
    Create code key corpus file.
    """

    ctr = Counter()
    with do_open(ifname, "r") as fobj:
        for line in fobj:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            keys = get_keys(obj)
            ctr.update(keys)

    key_code = []
    for i, (key, _) in enumerate(ctr.most_common()):
        code = short_code(i)
        key_code.append((code, key))

    with do_open(ofname, "w") as fobj:
        json.dump(key_code, fobj)

def main():
    from docopt import docopt
    args = docopt(__doc__)

    if args["mkdict"]:
        ifname = args["<CorpusFile>"]
        ofname = args["<CodeKeyFile>"]
        make_codekey_file(ifname, ofname)

    if args["encode"]:
        ifname = args["<DecodedFile>"]
        ofname = args["<EncodedFile>"]

        if args["-m"]:
            set_minifier(args["<CodeKeyFile>"])

        do_minify = args["-m"]
        do_compres = False
        drop_empty = args["-d"]
        with do_open(ifname, "r") as fin, do_open(ofname, "w") as fout:
            for line in fin:
                obj = json.loads(line)
                obj = dumps(obj, do_minify, do_compres, drop_empty)
                print(obj, file=fout)

    if args["decode"]:
        ifname = args["<EncodedFile>"]
        ofname = args["<DecodedFile>"]

        if args["-m"]:
            set_minifier(args["<CodeKeyFile>"])

        do_unminify = args["-m"]
        do_decompres = False
        with do_open(ifname, "r") as fin, do_open(ofname, "w") as fout:
            for line in fin:
                obj = loads(line, do_unminify, do_decompres)
                obj = json.dumps(obj, do_minify, do_compres, drop_empty)
                print(obj, file=fout)

if __name__ == '__main__':
    main()

