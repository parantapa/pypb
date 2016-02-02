# encoding: utf-8
"""
Serialize objects with msgpack and key code compression.

This replaces the string json keys with integers,
and then serializes with msgpack.
"""

from __future__ import division, print_function

from collections import Counter

from msgpack import packb as msgpack_packb, \
                    unpackb as msgpack_unpackb

def minify(obj, key_code):
    """
    Create a minified version of the object.
    """

    if isinstance(obj, dict):
        return {
            key_code[k]: minify(v, key_code)
            for k, v in obj.iteritems()
        }

    if isinstance(obj, (list, tuple)):
        return [minify(v, key_code) for v in obj]

    return obj

def unminify(obj, code_key):
    """
    Create a unminified version of the object.
    """

    if isinstance(obj, dict):
        return {
            code_key[k]: unminify(v, code_key)
            for k, v in obj.iteritems()
        }

    if isinstance(obj, (list, tuple)):
        return [unminify(v, code_key) for v in obj]

    return obj

def count_keys(obj):
    """
    Count the occurences of different keys in the object.
    """

    key_count = Counter()
    objstack = [obj]

    while objstack:
        obj = objstack.pop()

        if isinstance(obj, dict):
            key_count.update(obj.iterkeys())
            objstack.extend(obj.itervalues())

        elif isinstance(obj, (list, tuple)):
            objstack.extend(obj)

    return key_count

def make_codekey(key_count):
    """
    Create a mapping from key to code.
    """

    code_key = [k for k, _ in key_count.most_common()]
    key_code = {k: i for i, k in enumerate(code_key)}

    return code_key, key_code

def packb(obj):
    """
    Return the compressed object.
    """

    key_count = count_keys(obj)
    code_key, key_code = make_codekey(key_count)
    obj = minify(obj, key_code)
    obj = [code_key, obj]
    return msgpack_packb(obj, use_bin_type=True)

def unpackb(obj):
    """
    Return the decompressd objects.
    """

    obj = msgpack_unpackb(obj, encoding="utf-8")
    code_key, obj = obj
    return unminify(obj, code_key)
