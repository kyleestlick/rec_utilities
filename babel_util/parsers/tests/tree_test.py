#!/usr/bin/env python
import unittest
from parsers.tree import TreeFile, TreeRecord
import io


class TestTreeRecord(unittest.TestCase):
    def test_simple(self):
        tr = TreeRecord("1:2:3", "Potatoes!", .000119027)
        self.assertEqual(tr.local, "1:2")
        self.assertEqual(tr.pid, "Potatoes!")
        self.assertEqual(tr.parent, "1")
        self.assertEqual(tr.score, .000119027)
        self.assertEqual(tr, tr)
        tr2 = TreeRecord("1:2:3", "Potatoes!", .000119027)
        self.assertEqual(tr2, tr)
        tr3 = TreeRecord("1:2:4", "Potoes!", .000119027)
        self.assertNotEqual(tr2, tr3)

    def test_no_parent(self):
        tr = TreeRecord("1:3", "Potatoes!", .000119027)
        self.assertEqual(tr.local, "1")
        self.assertEqual(tr.pid, "Potatoes!")
        self.assertEqual(tr.parent, None)
        self.assertEqual(tr.score, .000119027)

    def test_bad_cluster(self):
        with self.assertRaises(ValueError) as cm:
            TreeRecord("1", "Potatoes", 1222)

        with self.assertRaises(ValueError) as cm:
            TreeRecord(None, "Potatoes", 1222)

        with self.assertRaises(AttributeError) as cm:
            TreeRecord(123, "Potatoes", 1222)

    def test_no_pid(self):
        with self.assertRaises(ValueError) as cm:
            TreeRecord("1:3", None, .000119027)

        with self.assertRaises(ValueError) as cm:
            TreeRecord("1:3", "", .000119027)

    def test_no_score(self):
        tr = TreeRecord("1:3", "Potatoes", 0)
        self.assertEqual(tr.score, -1.0)

        with self.assertRaises(ValueError) as cm:
            TreeRecord("1:3", "Potatoes", None)

        tr = TreeRecord("1:3", "Potatoes", ".321")
        self.assertEqual(tr.score, .321)


class TestTreeFile(unittest.TestCase):
    line_tests = [
        ('1:1:1:1 0.000119027 "1931976"', TreeRecord("1:1:1:1", "1931976", 0.000119027)),
        ('1:1:1:2 9.192e-05 "2407089"', TreeRecord("1:1:1:2", "2407089", 9.192e-05)),
        ('1:1:1:3 7.9441e-05 "1935620"', TreeRecord("1:1:1:3", "1935620", 7.9441e-05)),
        ('1:1:1:4 7.36738e-05 "2460305"', TreeRecord("1:1:1:4", "2460305", 7.36738e-05))]


    def setUp(self):
        buff = "\n".join(map(lambda x: x[0], self.line_tests))
        self.stream = io.StringIO(buff)

    def test_stream_parse(self):
        parser = TreeFile(self.stream)
        output = []
        for record in parser:
            output.append(record)

        self.assertEqual(output, [x[1] for x in self.line_tests], self.line_tests)

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

if __name__ == "__main__":
    unittest.main()
