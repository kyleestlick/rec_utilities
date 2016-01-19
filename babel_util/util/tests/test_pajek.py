#!/usr/bin/env python3
import unittest
import io
from util.PajekFactory import PajekFactory

class TestPajek(unittest.TestCase):
    def setUp(self):
        self._edge_stream = io.StringIO()
        self._node_stream = io.StringIO()
        self.pjk = PajekFactory(self._edge_stream, self._node_stream)

    def test_stream_write(self):
        self.pjk.add_edge("bob", "tim")
        self.pjk.add_edge("bob", "rob")
        self.pjk.add_edge("tim", "rob")
        self.pjk.add_edge("tim", "bob")
        NODE_LIST = ['0 "bob"\n', '1 "tim"\n', '2 "rob"\n']
        EDGE_LIST = ['0 1\n', '0 2\n', '1 2\n', '1 0\n']
        HEADERS = ('*vertices 3\n', '*edges 4\n')
        self._edge_stream.seek(0)
        self._node_stream.seek(0)
        self.assertListEqual(self._node_stream.readlines(), NODE_LIST)
        self.assertListEqual(self._edge_stream.readlines(), EDGE_LIST)
        self.assertTupleEqual(self.pjk.make_header(), HEADERS)

    def test_self_cite(self):
        self.pjk.add_edge("bob", "bob")
        self._edge_stream.seek(0)
        self._node_stream.seek(0)
        NODE_LIST = ['0 "bob"\n']
        EDGE_LIST = ['0 0\n']
        HEADERS = ('*vertices 1\n', '*edges 1\n')
        self.assertListEqual(self._node_stream.readlines(), NODE_LIST)
        self.assertListEqual(self._edge_stream.readlines(), EDGE_LIST)
        self.assertTupleEqual(self.pjk.make_header(), HEADERS)

if __name__ == "__main__":
    unittest.main()
