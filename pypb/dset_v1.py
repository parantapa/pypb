# encoding: utf-8
# pylint: disable=too-many-instance-attributes
"""
Read and write PB's Dataset files.

These are files which store blocks of msgpack objects,
compressed with zlib.

Data format:
    First 4KB:
        Magic string : "pb's dataset" (12 bytes)
        Version : little endian 2 byte unsigned integer
        Header size : little endian 4 byte unsigned integer
        Header: varaible sized msgpack object
            {
                "index_start": <location of the index block>,
                "index_size": <size of the index block>,
                "serializer": "msgpack",
                "compression": "lz4",
                "block_length": <number of items per block>,
                "length": <number of items in the dataset>.
            }
        Header checksum: 4 bytes


    Block: variable sized compressed msgpack object
    Block checksum: 4 bytes
    ...

    Index: variable sized compressed msgpack object
        [
            [block_start, block_size],
            ...
        ]
    Index checksum: 4 bytes
"""

import __builtin__
from collections import Sequence

from json import dumps as json_dumps, \
                 loads as json_loads
from zlib import compress as zlib_compress, \
                 decompress as zlib_decompress, \
                 adler32
from struct import pack as struct_pack, \
                   unpack as struct_unpack, \
                   calcsize

from msgpack import packb as msgpack_packb, \
                    unpackb as msgpack_unpackb
from lz4 import compress as lz4_compress, \
                decompress as lz4_decompress

import pypb.abs
from pypb.msgpackz import packb as msgpackz_packb, \
                          unpackb as msgpackz_unpackb

MAGIC_STRING = "pb's dataset"
HEADER_SPACE = 4096
PRE_HEADER_FMT = "< 12s H L"
CHECKSUM_LEN = 4
VERSION = 1

SERIALIZER_TABLE = {
    "msgpack": lambda x: msgpack_packb(x, use_bin_type=True),
    "json": lambda x: json_dumps(x, ensure_ascii=False, separators=(',',':')),
    "msgpackz": msgpackz_packb
}

UNSERIALIZER_TABLE = {
    "msgpack": lambda x: msgpack_unpackb(x, encoding="utf-8"),
    "json": json_loads,
    "msgpackz": msgpackz_unpackb
}

COMPRESSER_TABLE = {
    "zlib": lambda x: zlib_compress(x, 6),
    "lz4": lz4_compress,
}

DECOMPRESSER_TABLE = {
    "zlib": zlib_decompress,
    "lz4": lz4_decompress,
}

def checksum(data):
    chksum = adler32(data) & 0xffffffff
    chksum = struct_pack("<I", chksum)
    return chksum

def read_meta(fobj):
    """
    Read file header and index.
    """

    # Read the preheader
    fobj.seek(0)
    pre_header = fobj.read(calcsize(PRE_HEADER_FMT))
    magic, version, hdr_size = struct_unpack(PRE_HEADER_FMT, pre_header)
    if magic != MAGIC_STRING:
        raise IOError("Not a dataset file")
    if version < 1 or version > VERSION:
        raise IOError("Invalid version %d" % version)

    # Read the header
    header_raw = fobj.read(hdr_size)
    header_chksum = fobj.read(CHECKSUM_LEN)
    if checksum(header_raw) != header_chksum:
        raise IOError("Header checksum mismatch")
    header = msgpack_unpackb(header_raw, encoding="utf-8")

    # Get the decompresser function
    if header["compression"] not in DECOMPRESSER_TABLE:
        raise IOError("Unknown compression '%s'" % header["compression"])
    decompress = DECOMPRESSER_TABLE[header["compression"]]

    # Get the unserializer function
    if header["serializer"] not in UNSERIALIZER_TABLE:
        raise IOError("Unknown serializer '%s'" % header["serializer"])
    unserialize = UNSERIALIZER_TABLE[header["serializer"]]

    # Read the index
    fobj.seek(header["index_start"])
    index_raw = fobj.read(header["index_size"])
    index_chksum = fobj.read(CHECKSUM_LEN)
    if checksum(index_raw) != index_chksum:
        raise IOError("Index checksum mismatch")
    index_raw = decompress(index_raw)
    index = msgpack_unpackb(index_raw, encoding="utf-8")

    return version, header, decompress, unserialize, index

def load_block(fobj, index, n, decompress, unserialize):
    """
    Load the n-th block into memory.
    """

    assert 0 <= n < len(index)

    block_start, block_size = index[n]

    fobj.seek(block_start)
    block_raw = fobj.read(block_size)
    block_chksum = fobj.read(CHECKSUM_LEN)
    if checksum(block_raw) != block_chksum:
        raise IOError("Block %d checksum mismatch" % n)

    block_raw = decompress(block_raw)
    return unserialize(block_raw)

class DatasetReader(Sequence, pypb.abs.Close):
    """
    Reads a dataset object from a file.
    """

    def __init__(self, fname):
        self.fname = fname
        self.fobj = None
        self.fobj = __builtin__.open(fname, "rb")

        version, header, decompress, unserialize, index = read_meta(self.fobj)

        self.version = version
        self.header = header
        self.decompress = decompress
        self.unserialize = unserialize
        self.index = index
        self.block_length = self.header["block_length"]
        self.length = self.header["length"]

        # NOTE: Only used by get_idx
        # get_idxs and get_slice use their own local block storage
        self.cur_block_idx = -1
        self.cur_block = None

    def _load_block(self, i):
        return load_block(self.fobj, self.index, i, self.decompress, self.unserialize)

    @pypb.abs.runonce
    def close(self):
        if self.fobj is not None:
            self.fobj.close()

    def get_idx(self, n):
        """
        Get the value at given idx.
        """

        _block_length = self.block_length

        n = (self.length + n) if n < 0 else n
        if n < 0 or n >= self.length:
            raise IndexError("Index out of range")

        i = n // _block_length
        j = n % _block_length
        if self.cur_block_idx != i:
            self.cur_block = self._load_block(i)
            self.cur_block_idx = i
        return self.cur_block[j]

    def get_slice(self, *args):
        """
        Return iterable for the given range.
        """

        _block_length = self.block_length

        start, stop, step = slice(*args).indices(self.length)

        # Find the number of items in slice
        n = (stop - start) // step
        if n <= 0:
            return

        # Check if begin and end indexes are in range
        if start < 0 or start >= self.length:
            raise IndexError("Index out of range")
        end = start + (n - 1) * step
        if end < 0 or end >= self.length:
            raise IndexError("Index out of range")

        # Do the actual loop
        # This doesn't use the class's cur_block
        cur_block_idx = -1
        cur_block = None
        for n in xrange(start, stop, step):
            i = n // _block_length
            j = n % _block_length
            if cur_block_idx != i:
                cur_block = self._load_block(i)
                cur_block_idx = i
            yield cur_block[j]

    def get_idxs(self, ns):
        """
        Get the values at given idxs.

        NOTE: if the indexes are not sorted, performance may be really slow.
        """

        _block_length = self.block_length

        cur_block_idx = -1
        cur_block = None
        for n in ns:
            n = (self.length + n) if n < 0 else n
            if n < 0 or n >= self.length:
                raise IndexError("Index out of range")

            i = n // _block_length
            j = n % _block_length
            if cur_block_idx != i:
                cur_block = self._load_block(i)
                cur_block_idx = i
            yield cur_block[j]

    def __iter__(self):
        for i in xrange(len(self.index)):
            cur_block = self._load_block(i)
            for item in cur_block:
                yield item

    def __len__(self):
        return self.length

    def __getitem__(self, key):
        if isinstance(key, slice):
            return list(self.get_slice(key.start, key.stop, key.step))
        elif isinstance(key, (list, tuple)):
            return list(self.get_idxs(key))
        else:
            return self.get_idx(key)

class DatasetWriter(pypb.abs.Close):
    """
    Writes a dataset object to a file.
    """

    def __init__(self, fname, block_length,
                 compression="lz4", serializer="msgpack"):
        self.fname = fname
        self.fobj = None
        self.fobj = __builtin__.open(fname, "wb")

        block_length = int(block_length)
        if block_length < 1:
            raise ValueError("Block length must be at-least 1")
        self.block_length = block_length
        self.length = 0

        if compression not in COMPRESSER_TABLE:
            raise ValueError("Unknown compression '%s'" % compression)
        self.compression = compression
        self.compress = COMPRESSER_TABLE[compression]

        if serializer not in SERIALIZER_TABLE:
            raise ValueError("Unknown serializer '%s'" % serializer)
        self.serializer = serializer
        self.serialize = SERIALIZER_TABLE[serializer]

        self.cur_block = []
        self.cur_block_idx = 0

        self.index = []

        # Write the header region with garbage
        self.fobj.write(chr(42) * HEADER_SPACE)

    @pypb.abs.runonce
    def close(self):
        if self.fobj is not None:
            self._flush(force=True)
            index_start, index_size = self._write_index()
            self._write_header(index_start, index_size)
            self.fobj.close()

    def _flush(self, force=False):
        """
        Flush the current block to output file.
        """

        if len(self.cur_block) != self.block_length and not force:
            raise ValueError("Cant flush unfilled block without forcing")

        block_start = self.fobj.tell()
        block_raw = self.serialize(self.cur_block)
        block_raw = self.compress(block_raw)
        block_chksum = checksum(block_raw)

        self.index.append((block_start, len(block_raw)))

        self.fobj.write(block_raw)
        self.fobj.write(block_chksum)

        self.cur_block = []
        self.cur_block_idx += 1

    def _write_index(self):
        """
        Write the index to the file.

        Retuns index block start location and index block size.
        """

        index_start = self.fobj.tell()
        index_raw = msgpack_packb(self.index, use_bin_type=True)
        index_raw = self.compress(index_raw)
        index_chksum = checksum(index_raw)

        self.fobj.write(index_raw)
        self.fobj.write(index_chksum)

        return index_start, len(index_raw)

    def _write_header(self, index_start, index_size):
        """
        Write the header to the file.
        """

        header = {
            "index_start": index_start,
            "index_size": index_size,
            "serializer": self.serializer,
            "compression": self.compression,
            "block_length": self.block_length,
            "length": self.length
        }
        header_raw = msgpack_packb(header, use_bin_type=True)
        header_chksum = checksum(header_raw)

        magic = MAGIC_STRING
        version = VERSION
        hdr_size = len(header_raw)

        pre_header = struct_pack(PRE_HEADER_FMT, magic, version, hdr_size)

        assert len(pre_header) + len(header_raw) + len(header_chksum) < HEADER_SPACE

        self.fobj.seek(0)
        self.fobj.write(pre_header)
        self.fobj.write(header_raw)
        self.fobj.write(header_chksum)

    def append(self, item):
        if len(self.cur_block) == self.block_length:
            self._flush()
        self.cur_block.append(item)
        self.length += 1

    def extend(self, iterable):
        if len(self.cur_block) == self.block_length:
            self._flush()
        for item in iterable:
            self.cur_block.append(item)
            self.length += 1
            if len(self.cur_block) == self.block_length:
                self._flush()

class DatasetAppender(DatasetWriter):
    """
    Appends to a dataset file.
    """

    def __init__(self, fname): # pylint: disable=super-init-not-called
        self.fname = fname
        self.fobj = None
        self.fobj = __builtin__.open(fname, "r+b")

        _, header, decompress, unserializer, index = read_meta(self.fobj)

        self.block_length = header["block_length"]
        self.length = header["length"]

        if header["compression"] not in COMPRESSER_TABLE:
            raise ValueError("Unknown compression '%s'" % header["compression"])
        self.compression = header["compression"]
        self.compress = COMPRESSER_TABLE[header["compression"]]

        if header["serializer"] not in SERIALIZER_TABLE:
            raise ValueError("Unknown serializer '%s'" % header["serializer"])
        self.serializer = header["serializer"]
        self.serialize = SERIALIZER_TABLE[header["serializer"]]

        if len(index) > 0:
            self.cur_block_idx = len(index) - 1
            self.cur_block = load_block(self.fobj,
                                        index, self.cur_block_idx,
                                        decompress, unserializer)

            # Remove last entry from index
            index.pop()

            self.index = index
        else:
            self.cur_block_idx = 0
            self.cur_block = []
            self.index = []

        # Move the file pointer to after the current index data.
        # This will create holes in the file.
        # But it reduces the chances of total loss in case of crash.
        index_start = header["index_start"]
        index_size = header["index_size"]
        self.fobj.seek(index_start + index_size + CHECKSUM_LEN)

def open(fname, mode="r", block_length=None,       # pylint: disable=redefined-builtin
         compression="lz4", serializer="msgpack"):
    """
    Open a dataset for reading or writing.
    """

    if mode == "r":
        return DatasetReader(fname)
    elif mode == "w":
        if block_length is None:
            raise ValueError("Must specify block_length for write mode")
        return DatasetWriter(fname, block_length, compression, serializer)
    elif mode == "a":
        return DatasetAppender(fname)
    else:
        raise ValueError("Invalid mode '%s'" % mode)

def main():
    import sys
    import pprint

    usage = "./dset <fname.dset> [index]"

    if len(sys.argv) == 2:
        fname = sys.argv[1]
        with DatasetReader(fname) as dset:
            for item in dset:
                pprint.pprint(item)
    elif len(sys.argv) == 3:
        fname = sys.argv[1]
        key = sys.argv[2]

        # Using the multiple index notation
        if "," in key:
            key = key.strip().split(",")
            key = map(int, key)
            with DatasetReader(fname) as dset:
                for item in dset.get_idxs(key):
                    pprint.pprint(item)

        # Using the slice notation
        elif ":" in key:
            key = key.strip().split(":")
            key = [(int(k) if k.strip() else None) for k in key]
            with DatasetReader(fname) as dset:
                for item in dset.get_slice(*key):
                    pprint.pprint(item)

        # Using a single index
        idx = int(key)
        with DatasetReader(fname) as dset:
            pprint.pprint(dset.get_idx(idx))
    else:
        print usage
        sys.exit(0)

if __name__ == '__main__':
    main()