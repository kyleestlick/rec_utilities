#!/usr/bin/env python
import unittest
from aminer import AMinerParser

class AMinerParserTest(unittest.TestCase):
    SINGLE_TEST_FILE = "./aminer_single.txt"

    def setUp(self):
        self.single_test = open(self.SINGLE_TEST_FILE, "r")

    def test_single_parse(self):
        p = AMinerParser()

if __name__ == "__main__":
    unittest.main()
