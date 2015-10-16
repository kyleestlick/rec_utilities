#!/usr/bin/env python
import unittest
from link import LinkFile

class LinkFileTest(unittest.TestCase):
    def setUp(self):
        self.test_file = open("./test.edge")

    def tearDown(self):
        self.test_file.close()

    def test_build(self):
        q = LinkFile(self.test_file)
        s = q.to_sparse_matrix()

