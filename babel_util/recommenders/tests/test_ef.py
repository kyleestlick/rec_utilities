#!/usr/bin/env python3
import unittest
from recommenders.ef import make_expert_rec, make_classic_recs
from parsers.tree import TreeFile

EXPERT = [[18, 23, 24, 26, 27],
          [17, 15, 14, 13],
          [12, 16, 11, 10],
          [9, 20, 21, 25],
          [8, 6, 5, 4],
          [3, 7, 2, 1],
          [22, 19]]

CLASSIC = [[18, 23, 24, 26, 27, 17, 12, 15, 16, 11, 14, 13, 10],
           [9, 8, 20, 21, 25, 6, 3, 7, 5, 4, 2, 22, 1, 19]]


def make_answer(answers, current):
    r = None
    for a in answers:
        if int(current) in a:
            r = list(map(str, a.copy()))  # Make a copy and convert to strings
            r.remove(current)

    if not r:
        raise ValueError(current)

    return r[:10]


class TestEFRec(unittest.TestCase):
    def setUp(self):
        self.tr = TreeFile(open("./ninetriangles.tree", "r"))

    def test_classic(self):
        for recs in make_classic_recs(self.tr):
            recd = [r.pid for r in recs]
            self.assertListEqual(recd, make_answer(CLASSIC, recs[0].target_pid))

    def test_expert(self):
        for recs in make_expert_rec(self.tr):
            recd = [r.pid for r in recs]
            self.assertListEqual(recd, make_answer(EXPERT, recs[0].target_pid))
