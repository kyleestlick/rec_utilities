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


class WOSStream(ContentHandler):
    def __init__(self, stream):
        self.tree = ET.iterparse(stream)
        self.context = iter(self.tree)
        self._UID = make_ns_key("UID")
        self._uid = make_ns_key("uid")
        self._REC = make_ns_key("REC")
        self._title = make_ns_key("title")
        self._identifier = make_ns_key("identifier")
        self._pub_info = make_ns_key("pub_info")
        self.md = {"citations": []}

    def parse(self):
        for event, elem in self.tree:
            if elem.tag == self._UID:
                self.md["id"] = elem.text
            elif elem.tag == self._uid:
                self.md["citations"].append(elem.text)
            elif elem.tag == self._title:
                if elem.attrib["type"] == "item":
                    self.md["title"] = elem.text
                elif elem.attrib["type"] == "source":
                    self.md["publication"] = elem.text
            elif elem.tag == self._identifier and elem.attrib["type"] == "doi":
                self.md["doi"] = elem.attrib["value"]
            elif elem.tag == self._pub_info:
                self.md["date"] = elem.attrib["sortdate"]
                self.md["pub_type"] = elem.attrib["pubtype"]
            elif elem.tag == self._REC:
                yield self.md
                self.md.clear()
                self.md["citations"] = []
            elem.clear()

