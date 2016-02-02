# encoding: utf-8
# pylint: disable=redefined-outer-name
"""
Test the dset module.
"""

import random

import pytest

import pypb.dset

@pytest.fixture(params=[1000, 1001, 10000, 10001])
def test_data(request):
    return range(request.param)

@pytest.fixture(params=[10, 100, 7, 97])
def block_length(request):
    return request.param

def test_simple(tmpdir, test_data, block_length):
    """
    Test the different serializers and compression algos.
    """

    for compression in pypb.dset.COMPRESSER_TABLE.keys():
        for serializer in pypb.dset.SERIALIZER_TABLE.keys():

            fname = "test-%s-%s.dset" % (compression, serializer)
            fname = tmpdir.join(fname).strpath

            params = {
                "block_length": block_length,
                "compression": compression,
                "serializer": serializer
            }

            with pypb.dset.open(fname, "w", **params) as dset:
                dset.extend(test_data)

            with pypb.dset.open(fname) as dset:
                assert test_data == list(dset)

def test_append(tmpdir, test_data, block_length):
    """
    Test the iterator version.
    """

    fname = tmpdir.join("test.dset").strpath

    with pypb.dset.open(fname, "w", block_length) as dset:
        for item in test_data:
            dset.append(item)

    with pypb.dset.open(fname) as dset:
        assert test_data == list(dset)

def test_extend(tmpdir, test_data, block_length):
    """
    Test the iterator version.
    """

    fname = tmpdir.join("test.dset").strpath

    with pypb.dset.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with pypb.dset.open(fname) as dset:
        assert test_data == list(dset)

def test_append_mode_2parts(tmpdir, test_data, block_length):
    """
    Test the append mode usage.
    """

    fname = tmpdir.join("test.dset").strpath

    random.seed(42)
    for _ in xrange(10):
        n = random.randint(0, len(test_data) - 1)

        # Randomly split input data into two parts
        part1 = test_data[:n]
        part2 = test_data[n:]

        # Insert into dset separately
        with pypb.dset.open(fname, "w", block_length) as dset:
            dset.extend(part1)

        # Second time open in append mode
        with pypb.dset.open(fname, "a") as dset:
            dset.extend(part2)

        with pypb.dset.open(fname) as dset:
            assert test_data == list(dset)

def test_append_mode_3parts(tmpdir, test_data, block_length):
    """
    Test the append mode usage.
    """

    fname = tmpdir.join("test.dset").strpath

    random.seed(42)
    for _ in xrange(10):
        n1 = random.randint(0, len(test_data) - 1)
        n2 = random.randint(n1, len(test_data) - 1)

        # Randomly split input data into two parts
        part1 = test_data[:n1]
        part2 = test_data[n1:n2]
        part3 = test_data[n2:]

        # Insert into dset separately
        with pypb.dset.open(fname, "w", block_length) as dset:
            dset.extend(part1)

        # Second time open in append mode
        with pypb.dset.open(fname, "a") as dset:
            dset.extend(part2)

        # Second time open in append mode
        with pypb.dset.open(fname, "a") as dset:
            dset.extend(part3)

        with pypb.dset.open(fname) as dset:
            assert test_data == list(dset)

def test_empty_read_raises(tmpdir):
    """
    Test raise on empty file raises.
    """

    fname = tmpdir.join("test.dset").strpath

    with pytest.raises(IOError):
        with pypb.dset.open(fname):
            pass

    with pytest.raises(IOError):
        with pypb.dset.open(fname, "a"):
            pass

def test_append_mode_crash(tmpdir, test_data, block_length):
    """
    Simulate crash while appending.
    """

    fname = tmpdir.join("test.dset").strpath

    random.seed(42)
    for _ in xrange(10):
        n = random.randint(0, len(test_data) - 1)

        # Randomly split input data into two parts
        part1 = test_data[:n]
        part2 = test_data[n:]

        # Insert into dset separately
        with pypb.dset.open(fname, "w", block_length) as dset:
            dset.extend(part1)

        # Second time open in append mode
        with pypb.dset.open(fname, "a") as dset:
            dset.extend(part2)

            # This should stop the close function
            # from doing its job
            dset.fobj = None

        # We should get back the first part intact
        with pypb.dset.open(fname) as dset:
            assert part1 == list(dset)

def test_get_idx(tmpdir, test_data, block_length):
    """
    Test the get_idx version.
    """

    fname = tmpdir.join("test.dset").strpath

    with pypb.dset.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with pypb.dset.open(fname) as dset:
        for i in xrange(len(test_data)):
            assert test_data[i] == dset.get_idx(i)
            assert test_data[i] == dset[i]

            assert test_data[-i] == dset.get_idx(-i)
            assert test_data[-i] == dset[-i]

def test_get_idxs(tmpdir, test_data, block_length):
    """
    Test the multiple index version.
    """

    fname = tmpdir.join("test.dset").strpath

    with pypb.dset.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with pypb.dset.open(fname) as dset:
        random.seed(42)
        all_idxs = range(len(test_data))

        # Do ten random samples
        for _ in xrange(10):
            idxs = random.sample(all_idxs, len(all_idxs) // 10)
            idxs = sorted(idxs)

            vals = dset.get_idxs(idxs)
            for i, v in zip(idxs, vals):
                assert test_data[i] == v
            vals = dset[idxs]
            for i, v in zip(idxs, vals):
                assert test_data[i] == v

def test_get_slice(tmpdir, test_data, block_length):
    """
    Test slice version.
    """

    fname = tmpdir.join("test.dset").strpath

    with pypb.dset.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with pypb.dset.open(fname) as dset:
        random.seed(42)
        all_idxs = range(len(test_data))

        # Test the heads and tails
        for _ in xrange(10):
            n = random.choice(all_idxs)

            assert test_data[:n] == list(dset.get_slice(None, n))
            assert test_data[:n] == dset[:n]

            assert test_data[n:] == list(dset.get_slice(n, None))
            assert test_data[n:] == dset[n:]

def test_get_slice_wstep(tmpdir, test_data, block_length):
    """
    Test slice version with steps.
    """

    fname = tmpdir.join("test.dset").strpath

    with pypb.dset.open(fname, "w", block_length) as dset:
        dset.extend(test_data)

    with pypb.dset.open(fname) as dset:
        random.seed(42)
        all_idxs = range(len(test_data))

        # Do 100 times
        for _ in xrange(100):
            start, stop = sorted(random.sample(all_idxs, 2))

            # With half probability use negative step
            if random.random() < 0.5:
                step = -1

            # With half probability flip start, stop
            if random.random() < 0.5:
                stop, start = start, stop

            # With half probability do negative indexes
            if random.random() < 0.5:
                stop = stop - len(test_data)
                start = start - len(test_data)

            stop = stop + step

            assert test_data[start:stop:step] == list(dset.get_slice(start, stop, step))
            assert test_data[start:stop:step] == dset[start:stop:step]
