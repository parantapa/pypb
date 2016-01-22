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
                "compression": "zlib",
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
import zlib
import struct
from collections import defaultdict, Sequence

import msgpack
import lz4

import pypb.abs

MAGIC_STRING = "pb's dataset"
HEADER_SPACE = 4096
PRE_HEADER_FMT = "< 12s H L"
CHECKSUM_LEN = 4
VERSION = 1

COMPRESSER_TABLE = {
    "zlib": zlib.compress,
    "lz4": lz4.compress,
}

DECOMPRESSER_TABLE = {
    "zlib": zlib.decompress,
    "lz4": lz4.decompress,
}

def checksum(data):
    chksum = zlib.adler32(data) & 0xffffffff
    chksum = struct.pack("<I", chksum)
    return chksum

class DatasetReader(Sequence, pypb.abs.Close):
    """
    Reads a dataset object from a file.
    """

    def __init__(self, fname):
        self.fname = fname
        self.fobj = __builtin__.open(fname, "rb")

        # Read the preheader
        pre_header = self.fobj.read(struct.calcsize(PRE_HEADER_FMT))
        magic, version, hdr_size = struct.unpack(PRE_HEADER_FMT, pre_header)
        if magic != MAGIC_STRING:
            raise IOError("Not a dataset file")
        if version < 1 or version > VERSION:
            raise IOError("Invalid version %d" % version)
        self.version = version

        # Read the header
        header_raw = self.fobj.read(hdr_size)
        header_chksum = self.fobj.read(CHECKSUM_LEN)
        if checksum(header_raw) != header_chksum:
            raise IOError("Header checksum mismatch")
        header = msgpack.unpackb(header_raw, encoding="utf-8")

        self.header = header
        self.index_start = header["index_start"]
        self.index_size = header["index_size"]
        self.block_length = header["block_length"]
        self.length = header["length"]

        if header["compression"] not in DECOMPRESSER_TABLE:
            raise IOError("Unknown compression '%s'" % header["compression"])
        self.decompress = DECOMPRESSER_TABLE[header["compression"]]

        # Read the index
        self.fobj.seek(self.index_start)
        index_raw = self.fobj.read(self.index_size)
        index_chksum = self.fobj.read(CHECKSUM_LEN)
        if checksum(index_raw) != index_chksum:
            raise IOError("Index checksum mismatch")
        index_raw = self.decompress(index_raw)
        self.index = msgpack.unpackb(index_raw, encoding="utf-8")

        # NOTE: Only used by get_idx
        # get_idxs and get_slice use their own local block storage
        self.cur_block_idx = -1
        self.cur_block = None

    @pypb.abs.runonce
    def close(self):
        self.fobj.close()

    def _load_block(self, n):
        """
        Load the n-th block into memory.
        """

        assert 0 <= n < len(self.index)

        block_start, block_size = self.index[n]

        self.fobj.seek(block_start)
        block_raw = self.fobj.read(block_size)
        block_chksum = self.fobj.read(CHECKSUM_LEN)
        if checksum(block_raw) != block_chksum:
            raise IOError("Block %d checksum mismatch" % n)

        block_raw = self.decompress(block_raw)
        return msgpack.unpackb(block_raw, encoding="utf-8")

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
        """

        _block_length = self.block_length

        if not all(ns[i] < ns[i+1] for i in xrange(len(ns) -1)):
            raise ValueError("Indexes must be sorted and be unique")

        i_js = defaultdict(list)
        for n in ns:
            n = (self.length + n) if n < 0 else n
            if n < 0 or n >= self.length:
                raise IndexError("Index out of range")

            i = n // _block_length
            j = n % _block_length
            i_js[i].append(j)

        for i in sorted(i_js):
            cur_block = self._load_block(i)
            for j in i_js[i]:
                yield cur_block[j]

    def __iter__(self):
        return self.get_slice(self.length)

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

    def __init__(self, fname, block_length, compression="lz4"):
        self.fname = fname
        self.fobj = __builtin__.open(fname, "wb")

        block_length = int(block_length)
        if block_length < 1:
            raise ValueError("Block length must be at-least 1")
        self.block_length = block_length

        if compression not in COMPRESSER_TABLE:
            raise ValueError("Unknown compression '%s'" % compression)
        self.compression = compression
        self.compress = COMPRESSER_TABLE[compression]

        self.length = 0
        self.cur_block = []
        self.cur_block_idx = 0

        self.index = []

    @pypb.abs.runonce
    def close(self):
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

        # Write the header region with garbage
        if self.cur_block_idx == 0:
            self.fobj.write(chr(42) * HEADER_SPACE)

        block_start = self.fobj.tell()
        block_raw = msgpack.packb(self.cur_block, use_bin_type=True)
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
        index_raw = msgpack.packb(self.index, use_bin_type=True)
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
            "serializer": "msgpack",
            "compression": self.compression,
            "block_length": self.block_length,
            "length": self.length
        }
        header_raw = msgpack.packb(header, use_bin_type=True)
        header_chksum = checksum(header_raw)

        magic = MAGIC_STRING
        version = VERSION
        hdr_size = len(header_raw)

        pre_header = struct.pack(PRE_HEADER_FMT, magic, version, hdr_size)

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

def open(fname, mode="r", block_length=None, compression="lz4"): # pylint: disable=redefined-builtin
    """
    Open a dataset for reading or writing.
    """

    if mode == "r":
        return DatasetReader(fname)
    elif mode == "w":
        if block_length is None:
            raise ValueError("Must specify block_length for write mode")
        return DatasetWriter(fname, block_length, compression)
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
