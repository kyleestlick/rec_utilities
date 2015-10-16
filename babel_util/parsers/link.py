#!/usr/bin/env python
from collections import defaultdict
from scipy.sparse import dok_matrix
import numpy as np

#TODO: Figure out how to do this more safely

#Such hack
current_idx = -1
def accum():
    global current_idx
    current_idx += 1
    return current_idx


class LinkFile(object):
    """Parser for link files/edge lists."""

    def __init__(self, stream, comment='#', delimiter='\t'):
        self._stream = stream
        self.comment = comment
        self.delimiter = delimiter
        self.ids = defaultdict(accum)
        self._next_id = 0

    def build_ids(self):
        for fid, tid, _ in self:
            self.ids[fid]
            self.ids[tid]

    def to_sparse_matrix(self, dtype=np.int32):
        """Parse the stream and return a CSR sparse matrix"""
        cur_pos = self._stream.tell()
        self.build_ids()
        self._stream.seek(cur_pos)

        s = dok_matrix((len(self.ids), len(self.ids)), dtype=dtype)

        for fid, tid, weight in self:
            s[self.ids[fid], self.ids[tid]] += weight

        return s.tocsr()

    def __iter__(self):
        self._iter = iter(self._stream)
        return self

    def __next__(self):
        return self.next()

    def next(self):
        line = self._iter.__next__()
        while self.comment and line.startswith(self.comment):
            line = self._iter.__next__()

        # If a score isn't included, use a weight of 1
        retv = self.parse_line(line)
        if len(retv) == 2:
            retv.append(1)
        else:
            retv[2] = int(retv[2])
        return retv

    def parse_line(self, line):
        return list(map(str.strip, line.split(self.delimiter)))



