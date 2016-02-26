# encoding: utf-8
# pylint: disable=redefined-outer-name
"""
Test the dset_sort module.
"""

import sys
import random

import pytest

from pypb.dset_sort import dset_sort, self_key
import pypb.dset

@pytest.fixture(params=[1000, 1001, 10000, 10001])
def test_data(request):
    return [random.randint(0, sys.maxint) for _ in xrange(request.param)]

@pytest.fixture(params=[10, 100, 7, 97])
def block_length(request):
    return request.param

def test_dset_sort(tmpdir, test_data, block_length):
    """
    Test the sorting algo.
    """

    ifname1 = tmpdir.join("test-raw.dset").strpath
    ifname2 = tmpdir.join("test-sorted.dset").strpath

    with pypb.dset.open(ifname1, "w", block_length=block_length) as dset:
        dset.extend(test_data)

    with pypb.dset.open(ifname1) as din:
        with pypb.dset.open(ifname2, "w", block_length=block_length) as dout:
            dset_sort(din, dout, self_key, 10)

    with pypb.dset.open(ifname2) as dset:
        assert sorted(test_data) == list(dset)

