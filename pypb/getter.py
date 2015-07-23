# encoding: utf-8
"""
Create getters with multipart keys.
"""

def simple_itemgetter(key):
    """
    Get the value for the given key.
    """

    # Make sure we dont have any ..
    assert key.count("..") == 0

    # Split the pieces
    ks = key.split(".")

    # Make sure all pieces are non empty
    for k in ks:
        if not k:
            raise ValueError("Key part cannot be empty")

    ndots = len(ks) - 1
    if ndots == 0:

        def func(obj):
            return obj.get(key, None)
    elif ndots == 1:
        k1, k2 = ks

        def func(obj):
            obj = obj.get(k1, None)
            if obj is None: return None
            return obj.get(k2, None)
    elif ndots == 2:
        k1, k2, k3 = ks

        def func(obj): # pylint: disable=missing-docstring
            obj = obj.get(k1, None)
            if obj is None: return None
            obj = obj.get(k2, None)
            if obj is None: return None
            return obj.get(k3, None)
    else:
        raise NotImplementedError("Cant handle more than two '.'")

    return func

def deep_itemgetter(key):
    """
    Get the value for the given key.
    """

    nddots = key.count("..")
    if nddots == 0:
        func = simple_itemgetter(key)
    elif nddots == 1:
        k1, k2 = key.split("..")
        fn1 = simple_itemgetter(k1)
        fn2 = simple_itemgetter(k2)

        def func(obj):
            obj = fn1(obj)
            if obj is None: return None
            return [fn2(x) for x in obj]
    elif nddots == 2:
        k1, k2, k3 = key.split("..")
        fn1 = simple_itemgetter(k1)
        fn2 = simple_itemgetter(k2)
        fn3 = simple_itemgetter(k3)

        def func(obj): # pylint: disable=missing-docstring
            obj = fn1(obj)
            if obj is None: return None
            obj = [fn2(x) for x in obj]
            if not obj: return []
            return [[fn3(x) for x in xs] for xs in obj]
    else:
        raise NotImplementedError("Cant handle more than two '..'")

    return func

def make_getter(key):
    """
    Return the function to call for the key.
    """

    return deep_itemgetter(key)
