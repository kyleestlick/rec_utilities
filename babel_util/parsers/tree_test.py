#!/usr/bin/env python
import unittest
from tree import TreeFile

class TestTreeFile(unittest.TestCase):

    line_tests = [
        ('1:1:1:1 0.000119027 "1931976"', ("1:1:1:1", 0.000119027, "1931976")),
        ('1:1:1:2 9.192e-05 "2407089"'  , ("1:1:1:2", 9.192e-05, "2407089")),
        ('1:1:1:3 7.9441e-05 "1935620"' , ("1:1:1:3", 7.9441e-05, "1935620")),
        ('1:1:1:4 7.36738e-05 "2460305"', ("1:1:1:4", 7.36738e-05, "2460305"))]

    def test_line_parse(self):
        parse = TreeFile("Nahh")
        for test in TestTreeFile.line_tests:
            results = parse.parse_line(test[0])
            self.assertEqual(results, test[1])

    def test_iter(self):
        input_stream = [x[0] for x in self.line_tests]
        parse = TreeFile(input_stream)
        output = []
        for record in parse:
            output.append(record)

        self.assertEqual(output, [x[1] for x in self.line_tests], self.line_tests)

    def test_to_graph(self):
        input_stream = [x[0] for x in self.line_tests]
        
        G = TreeFile(input_stream).to_graph()
        self.assertEqual(G.number_of_nodes(), 7)
        self.assertEqual(G.number_of_edges(), 6)
        self.assertEqual(G.successors("1"), ['1:1'])
        self.assertEqual(len(G.successors("1:1:1")), 4)
        self.assertEqual(G.node["paper-2460305"]["score"], 7.36738e-05)
        self.assertEqual(G.predecessors("paper-2460305"), ["1:1:1"])

    def test_to_graph_orphan(self):
        G = TreeFile(['1:1 0 "1931976"']).to_graph()
        self.assertEqual(G.number_of_nodes(), 2)
        self.assertEqual(G.number_of_edges(), 1)

if __name__ == "__main__":
    unittest.main()
