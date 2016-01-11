#!/usr/bin/env python3
from collections import defaultdict
from functools import partial

def invert_dict(d):
    return dict(zip(d.values(), d.keys()))

class AutoID(object):
    """A dictionary that maps arbitrary names to sequential ids"""

    def __init__(self, first_id=0):
        """Initialize an AutoID dictionary

        Args:
            first_id: The first id to use

        Returns:
            An AutoID, a dictionary like object.

        """

        self.ids = defaultdict(partial(AutoID.accum, self))
        self.invert = dict()
        self._next_id = first_id

    def accum(self):
        self._next_id += 1
        return self._next_id - 1

    def finalize(self):
        self.ids = dict(self.ids)

    def lookup_id(self, id):
        raise "Not implemented"

    def values(self):
        return self.ids.values()

    def items(self):
        return self.ids.items()

    def keys(self):
        return self.ids.keys()

    def __getitem__(self, key):
        return self.ids[key]

    def __setitem__(self, key, value):
        self.ids[key] = value

    def __len__(self):
        return len(self.ids)

