#!/usr/bin/env python3
import unittest
from parsers.wos import WOSTree, WOSStream

SMALL_XML = "test_small.xml"
SMALL_PARSED = {'id': "WOS:000334657000026",
                'citations': ['WOS:000334657000026.1']}

MEDIUM_XML = "test_medium.xml"

class TestWOS(unittest.TestCase):
    def test_parse_small(self):
        parser = WOSTree(SMALL_XML)
        for entry in parser.parse():
            self.assertDictEqual(entry, SMALL_PARSED)

    def test_parse_medium(self):
        parser = WOSTree(MEDIUM_XML)
        for entry in parser.parse():
            print(entry)

class TestWOSStream(unittest.TestCase):
    def test_parse_small(self):
        parser = WOSStream(SMALL_XML)
        for entry in parser.parse():
            self.assertDictEqual(entry, SMALL_PARSED)

    def test_parse_medium(self):
        parser = WOSStream(MEDIUM_XML)
        for entry in parser.parse():
            print(entry)

if __name__ == '__main__':
    unittest.main()
