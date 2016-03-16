# encoding: utf-8
# pylint: disable=too-many-instance-attributes
"""
Read and write PB's Dataset files.

These are files which store blocks of msgpack objects,
compressed with zlib.

Data format:
    First 4KB:
        Magic string: "pb's dataset" (12 bytes)
        Version: little endian 2 byte unsigned integer
        Header size: little endian 4 byte unsigned integer
        Header checksum: 4 bytes + 4 bytes
        Header: varaible sized msgpack object
            {
                "index_start": <location of the index block>,
                "index_size": <size of the index block>,
                "index_size_raw": <size of the index block uncompressed>,
                "serializer": "msgpack",
                "compression": "lz4",
                "block_length": <number of items per block>,
                "length": <number of items in the dataset>.
            }

    Block checksum: 4 bytes + 4 bytes
    Block: variable sized compressed msgpack object
    ...

    Index checksum: 4 bytes + 4 bytes
    Index: variable sized compressed msgpack object
        [
            [block_start, block_size, block_size_raw],
            ...
        ]
"""

import __builtin__
from collections import Sequence

from zlib import compress as _zlib_compress, \
                 decompress as zlib_decompress, \
                 adler32
from struct import pack as struct_pack, \
                   unpack as struct_unpack, \
                   calcsize

from msgpack import packb as _msgpack_packb, \
                    unpackb as _msgpack_unpackb
from lz4 import compress as lz4_compress, \
                decompress as lz4_decompress

import pypb.abs

def zlib_compress(x):
    return _zlib_compress(x, 6)

def msgpack_packb(x):
    return _msgpack_packb(x, use_bin_type=True)

def msgpack_unpackb(x):
    return _msgpack_unpackb(x, encoding="utf-8")

MAGIC_STRING = "pb's dataset"
HEADER_SPACE = 4096
PRE_HEADER_FMT = "< 12s H L"
CHECKSUM_FMT = "< I I"
CHECKSUM_LEN = calcsize(CHECKSUM_FMT)
VERSION = 3

SERIALIZER_TABLE = {
    "msgpack": msgpack_packb
}

UNSERIALIZER_TABLE = {
    "msgpack": msgpack_unpackb
}

COMPRESSER_TABLE = {
    "zlib": zlib_compress,
    "lz4": lz4_compress,
}

DECOMPRESSER_TABLE = {
    "zlib": zlib_decompress,
    "lz4": lz4_decompress,
}

def checksum(data):
    size = len(data)
    chksum = adler32(data) & 0xffffffff
    return struct_pack(CHECKSUM_FMT, size, chksum)

def read_preheader(fobj):
    """
    Read the pre-header and return version and header_size
    """

    fobj.seek(0)
    pre_header = fobj.read(calcsize(PRE_HEADER_FMT))
    magic, version, hdr_size = struct_unpack(PRE_HEADER_FMT, pre_header)
    if magic != MAGIC_STRING:
        raise IOError("Not a dataset file")

    return version, hdr_size

def read_header(fobj):
    """
    Read the header
    """

    version, hdr_size = read_preheader(fobj)
    if version != VERSION:
        raise IOError("Invalid version %d" % version)

    # Read the header
    header_chksum = fobj.read(CHECKSUM_LEN)
    header_raw = fobj.read(hdr_size)
    if checksum(header_raw) != header_chksum:
        raise IOError("Header checksum mismatch")
    header = msgpack_unpackb(header_raw)

    # Get the decompresser function
    if header["compression"] not in DECOMPRESSER_TABLE:
        raise IOError("Unknown compression '%s'" % header["compression"])
    decompress = DECOMPRESSER_TABLE[header["compression"]]

    # Get the unserializer function
    if header["serializer"] not in UNSERIALIZER_TABLE:
        raise IOError("Unknown serializer '%s'" % header["serializer"])
    unserialize = UNSERIALIZER_TABLE[header["serializer"]]

    return version, header, decompress, unserialize

def read_index(fobj, index_start, index_size, decompress):
    """
    Read the index.
    """

    fobj.seek(index_start)
    index_chksum = fobj.read(CHECKSUM_LEN)
    index_comp = fobj.read(index_size)
    if checksum(index_comp) != index_chksum:
        raise IOError("Index checksum mismatch")
    index_raw = decompress(index_comp)
    index = msgpack_unpackb(index_raw)

    return index

def read_block(fobj, index, n, decompress):
    """
    Load the n-th block into memory.
    """

    assert 0 <= n < len(index)

    block_start, block_size, _ = index[n]

    fobj.seek(block_start)
    block_chksum = fobj.read(CHECKSUM_LEN)
    block_comp = fobj.read(block_size)
    if checksum(block_comp) != block_chksum:
        raise IOError("Block %d checksum mismatch" % n)

    block_raw = decompress(block_comp)
    return block_raw

class DatasetReader(Sequence, pypb.abs.Close):
    """
    Reads a dataset object from a file.
    """

    def __init__(self, fname):
        self.fname = fname
        self.fobj = None
        self.fobj = __builtin__.open(fname, "rb")

        version, header, decompress, unserialize = read_header(self.fobj)
        self.version = version
        self.header = header
        self.decompress = decompress
        self.unserialize = unserialize

        index_start = header["index_start"]
        index_size = header["index_size"]
        index = read_index(self.fobj, index_start, index_size, decompress)
        self.index = index

        self.block_length = self.header["block_length"]
        self.length = self.header["length"]

        # NOTE: Only used by get_idx
        # get_idxs and get_slice use their own local block storage
        self.cur_block_idx = -1
        self.cur_block = None

    def _load_block(self, i):
        block_raw = read_block(self.fobj, self.index, i, self.decompress)
        return msgpack_unpackb(block_raw)

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
        return self.unserialize(self.cur_block[j])

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
            yield self.unserialize(cur_block[j])

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
            yield self.unserialize(cur_block[j])

    def __iter__(self):
        for i in xrange(len(self.index)):
            cur_block = self._load_block(i)
            for item in cur_block:
                yield self.unserialize(item)

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
            index_start, index_size, index_size_raw = self._write_index()
            self._write_header(index_start, index_size, index_size_raw)
            self.fobj.close()

    def _flush(self, force=False):
        """
        Flush the current block to output file.
        """

        if len(self.cur_block) != self.block_length and not force:
            raise ValueError("Cant flush unfilled block without forcing")

        # Dont write empty blocks
        # This happens when current block is empty and file is closed.
        if not self.cur_block:
            return

        block_start = self.fobj.tell()
        block_raw = msgpack_packb(self.cur_block)
        block_comp = self.compress(block_raw)
        block_chksum = checksum(block_comp)

        self.index.append((block_start, len(block_comp), len(block_raw)))

        self.fobj.write(block_chksum)
        self.fobj.write(block_comp)

        self.cur_block = []
        self.cur_block_idx += 1

    def _write_index(self):
        """
        Write the index to the file.

        Retuns index block start location and index block size.
        """

        index_start = self.fobj.tell()
        index_raw = msgpack_packb(self.index)
        index_comp = self.compress(index_raw)
        index_chksum = checksum(index_comp)

        self.fobj.write(index_chksum)
        self.fobj.write(index_comp)

        return index_start, len(index_comp), len(index_raw)

    def _write_header(self, index_start, index_size, index_size_raw):
        """
        Write the header to the file.
        """

        header = {
            "index_start": index_start,
            "index_size": index_size,
            "index_raw": index_size_raw,
            "serializer": self.serializer,
            "compression": self.compression,
            "block_length": self.block_length,
            "length": self.length
        }
        header_raw = msgpack_packb(header)
        header_chksum = checksum(header_raw)

        magic = MAGIC_STRING
        version = VERSION
        hdr_size = len(header_raw)

        pre_header = struct_pack(PRE_HEADER_FMT, magic, version, hdr_size)

        assert len(pre_header) + len(header_raw) + len(header_chksum) < HEADER_SPACE

        self.fobj.seek(0)
        self.fobj.write(pre_header)
        self.fobj.write(header_chksum)
        self.fobj.write(header_raw)

    def append(self, item):
        if len(self.cur_block) == self.block_length:
            self._flush()
        self.cur_block.append(self.serialize(item))
        self.length += 1

    def extend(self, iterable):
        if len(self.cur_block) == self.block_length:
            self._flush()
        for item in iterable:
            self.cur_block.append(self.serialize(item))
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

        _, header, decompress, _ = read_header(self.fobj)

        index_start = header["index_start"]
        index_size = header["index_size"]
        index = read_index(self.fobj, index_start, index_size, decompress)

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
            # The last block is not full
            if self.length % self.block_length != 0:
                self.cur_block_idx = len(index) - 1
                block_raw = read_block(self.fobj,
                                       index, self.cur_block_idx,
                                       decompress)
                self.cur_block = msgpack_unpackb(block_raw)

                # Remove last entry from index
                index.pop()

                self.index = index

            else:
                self.cur_block_idx = len(index)
                self.cur_block = []
                self.index = index
        else:
            self.cur_block_idx = 0
            self.cur_block = []
            self.index = []

        # Move the file pointer to after the current index data.
        # This will create holes in the file.
        # But it reduces the chances of total loss in case of crash.
        self.fobj.seek(index_start + CHECKSUM_LEN + index_size)

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
