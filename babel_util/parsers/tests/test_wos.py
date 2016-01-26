#!/usr/bin/env python3
import unittest
import pprint
from parsers.wos import WOSStream

NOWOS_XML = "test_nowos.xml"
SMALL_XML = "test_small.xml"
SMALL_PARSED = {'id': "WOS:000334657000026",
                'title': "In Memoriam: Harry Meinardi (February 20, 1932-December 20, 2013)",
                'doi': '10.1111/epi.12578',
                'date': '2014-04-01',
                'publication': 'EPILEPSIA',
                'pub_type': 'Journal',
                'authors': ['Perucca, E', 'Reynolds, EH'],
                'abstract': None,
                'keywords': [],
                'citations': ['WOS:000334657000026.1']}

MEDIUM_XML = "test_medium.xml"


class TestWOSStream(unittest.TestCase):
    def setUp(self):
        self._pp = pprint.PrettyPrinter(indent=2)

    def test_parse_small(self):
        parser = WOSStream(SMALL_XML)
        for entry in parser.parse():
            self.assertDictEqual(entry, SMALL_PARSED)

    def test_parse_medium(self):
        parser = WOSStream(MEDIUM_XML)
        for entry in parser.parse():
            self._pp.pprint(entry)

    def test_parse_nowos(self):
        parser = WOSStream(NOWOS_XML, True)
        for entry in parser.parse():
            raise "This shouldn't ever happen"

        parser = WOSStream(SMALL_XML, True)
        for entry in parser.parse():
            self.assertDictEqual(entry, SMALL_PARSED)

if __name__ == '__main__':
    unittest.main()
