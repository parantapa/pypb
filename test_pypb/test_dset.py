# encoding: utf-8
# pylint: disable=redefined-outer-name
"""
Test the dset module.
"""

import random

import pytest

from pypb.dset import DatasetReader, DatasetWriter

@pytest.fixture(params=[1000, 1001, 10000, 10001])
def test_data(request):
    return range(request.param)

@pytest.fixture(params=[10, 100, 7, 97])
def block_length(request):
    return request.param

def test_simple(tmpdir, test_data, block_length):
    """
    Test the iterator version.
    """

    fname = tmpdir.join("test.dset").strpath

    with DatasetWriter(fname, block_length) as dset:
        for item in test_data:
            dset.append(item)

    with DatasetReader(fname) as dset:
        assert test_data == list(dset)

def test_iter(tmpdir, test_data, block_length):
    """
    Test the iterator version.
    """

    fname = tmpdir.join("test.dset").strpath

    with DatasetWriter(fname, block_length) as dset:
        dset.extend(test_data)

    with DatasetReader(fname) as dset:
        assert test_data == list(dset)

def test_get_idx(tmpdir, test_data, block_length):
    """
    Test the get_idx version.
    """

    fname = tmpdir.join("test.dset").strpath

    with DatasetWriter(fname, block_length) as dset:
        dset.extend(test_data)

    with DatasetReader(fname) as dset:
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

    with DatasetWriter(fname, block_length) as dset:
        dset.extend(test_data)

    with DatasetReader(fname) as dset:
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

    with DatasetWriter(fname, block_length) as dset:
        dset.extend(test_data)

    with DatasetReader(fname) as dset:
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

    with DatasetWriter(fname, block_length) as dset:
        dset.extend(test_data)

    with DatasetReader(fname) as dset:
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
