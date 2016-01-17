#!/usr/bin/env python3
import unittest
import io
from util.PajekFactory import PajekFactory

class TestPajek(unittest.TestCase):
    def setUp(self):
        self._edge_stream = io.StringIO()
        self._node_stream = io.StringIO()

    def test_stream_write(self):
        pjk = PajekFactory(self._edge_stream, self._node_stream)
        pjk.add_edge("bob", "tim")
        pjk.add_edge("bob", "rob")
        pjk.add_edge("tim", "rob")
        pjk.add_edge("tim", "bob")
        NODES_LIST = ['1 "bob"\n', '2 "tim"\n', '3 "rob"\n']
        EDGE_LIST = ['1 2\n', '1 3\n', '2 3\n', '2 1\n']
        HEADERS = ('*vertices 3\n', '*edges 4\n')
        self._edge_stream.seek(0)
        self._node_stream.seek(0)
        self.assertListEqual(self._node_stream.readlines(), NODES_LIST)
        self.assertListEqual(self._edge_stream.readlines(), EDGE_LIST)
        self.assertTupleEqual(pjk.make_header(), HEADERS)

if __name__ == "__main__":
    unittest.main()
