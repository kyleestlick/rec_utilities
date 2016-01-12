#!/usr/bin/env python
import xml.etree.ElementTree as ET
from xml.sax import ContentHandler

WOS_NS = {"wok": "http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord"}


def make_ns_key(key):
    """Build the appropriately namespaced key for the WOS collection"""
    return "{{{0}}}{1}".format(WOS_NS["wok"], key)


def parse_reference(ref):
    #NOTE: uid **should** be lowercase here. So don't uppercase it!
    return ref.findtext("wok:uid", namespaces=WOS_NS)


class WOSTree(object):
    """Parser for Thompson-Reuters WebOfScience"""

    def __init__(self, stream):
        self.tree = ET.parse(stream)

    def parse(self):
        for child in self.tree.getroot():
            md = {"id": child.findtext("wok:UID", namespaces=WOS_NS),
                  "citations": list(filter(None, map(parse_reference, child.findall(".//wok:reference", namespaces=WOS_NS))))} #Filter to remove empty references <reference/>
            yield md

class WOSStream(ContentHandler):
    def __init__(self, stream):
        self.tree = ET.iterparse(stream)
        self.context = iter(self.tree)
        self._UID = make_ns_key("UID")
        self._uid = make_ns_key("uid")
        self._REC = make_ns_key("REC")

    def parse(self):
        md = {"citations": []}
        for event, elem in self.tree:
            if elem.tag == self._UID:
                md["id"] = elem.text
            elif elem.tag == self._uid:
                md["citations"].append(elem.text)
            elif elem.tag == self._REC:
                yield md
                md.clear()
                md["citations"] = []
            elem.clear()

