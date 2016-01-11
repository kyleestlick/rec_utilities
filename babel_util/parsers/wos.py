#!/usr/bin/env python
import xml.etree.ElementTree as ET


def parse_reference(ref):
    #NOTE: uid **should** be lowercase here. So don't uppercase it!
    return ref.findtext("wok:uid", namespaces=WOS.ns)


class WOS(object):
    """Parser for Thompson-Reuters WebOfScience"""
    ns = {"wok": "http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord"}

    def __init__(self, stream):
        self.tree = ET.parse(stream)

    def parse(self):
        for child in self.tree.getroot():
            md = {"id": child.findtext("wok:UID", namespaces=WOS.ns),
                  "citations": list(map(parse_reference, child.findall(".//wok:reference", namespaces=WOS.ns)))}
            yield md

