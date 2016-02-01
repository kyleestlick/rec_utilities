#!/usr/bin/env python3
import unittest
import io
from util.PajekFactory import PajekFactory

class TestPajek(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self._node_stream = io.StringIO()
        self.pjk = PajekFactory()

    def test_stream_write(self):
        self.pjk.add_edge("bob", "tim")
        self.pjk.add_edge("bob", "rob")
        self.pjk.add_edge("tim", "rob")
        self.pjk.add_edge("tim", "bob")
        self.pjk.write(self.output)
        self.output.seek(0)
        self.assertListEqual(self.output.readlines(), ['*vertices 3\n', '1 "bob"\n', '2 "tim"\n', '3 "rob"\n', '*edges 4\n', '1 2\n', '1 3\n', '2 3\n', '2 1\n'])

    def test_self_cite(self):
        self.pjk.add_edge("bob", "bob")
        self.pjk.write(self.output)
        self.output.seek(0)
        self.assertListEqual(self.output.readlines(), ['*vertices 1\n', '1 "bob"\n', '*edges 1\n', '1 1\n'])

if __name__ == "__main__":
    unittest.main()
