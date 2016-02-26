# encoding: utf-8
"""
Sort a dset file by key.

This can be used when every object in a dset file
can be identified with an integer key.
"""

from __future__ import division, print_function

import numpy as np
from pypb.progress import progress

def first_idx_key(x):
    return x[0]

def id_key(x):
    return x["id"]

def self_key(x):
    return x

def dset_sort(din, dout, keyfn, cache_blocks, show_progress=False):
    # pylint: disable=protected-access
    """
    Sort the input dset into the output dset.

    din          - dset file containing objects to be sorted.
    dout         - dset file where objects are to be written in sorted order.
    keyfn        - a function which returns the key for every object.
    cache_blocks - number of blocks to cache.
    """

    # Load the keys
    if show_progress:
        print("Loading keys ...")
    keys = np.empty(len(din), dtype=int)
    for srci, obj in enumerate(din):
        try:
            keys[srci] = keyfn(obj)
        except (KeyError, IndexError):
            msg = "Error getting key for object: %d" % srci
            raise ValueError(msg)

    # Get the sorted ordering
    if show_progress:
        print("Sorting keys ...")
    key_order = np.argsort(keys)

    # Free up the space for keys
    # We wont need it anymore
    del keys

    # Compute the cache size
    cache_size = int(cache_blocks * din.block_length)

    # NOTE: We do our own looping
    # We access _load_block method of the src dataset
    cur_block_idx = -1
    cur_block = None
    _block_length = din.block_length

    if show_progress:
        pass_count = len(din) // cache_size
        if len(din) % cache_size:
            pass_count += 1
        iterable = xrange(0, len(din), cache_size)
        print("Total pass needed:", pass_count)
        msg = ("Passes done {count:,d} ({percentage:.1f} %) "
               "- elapsed: {elapsed} - eta: {eta} "
               "- {speed:.2f} loops/sec")
        iterable = progress(iterable, msg=msg, total=pass_count, logfn=print)
    else:
        iterable = xrange(0, len(din), cache_size)

    for start in iterable:
        idxs = key_order[start:start + cache_size]
        idxs = [(int(srci), dsti) for dsti, srci in enumerate(idxs)]
        idxs.sort(key=lambda x: x[0])

        cache = [None] * len(idxs)
        for srci, dsti in idxs:
            i = srci // _block_length
            j = srci % _block_length

            if cur_block_idx != i:
                cur_block = din._load_block(i)
                cur_block_idx = i
            cache[dsti] = cur_block[j]

        dout.extend(cache)

        # Free up the cache
        del cache
