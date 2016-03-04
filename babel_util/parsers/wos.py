#!/usr/bin/env python
import xml.etree.ElementTree as ET
from xml.sax import ContentHandler
from random import uniform
import datetime

WOS_NS = "http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord"
SKIP_LEN = len(WOS_NS) + 2  # {} enclose the namespace


def make_ns_key(key):
    """Build the appropriately namespaced key for the WOS collection"""
    return "{{{0}}}{1}".format(WOS_NS, key)


def parse_pubinfo(x):
    e, md = x
    if e.tag == "pub_info":
        md["date"] = datetime.datetime.strptime(e.attrib["sortdate"], "%Y-%m-%d")
        md["pub_type"] = e.attrib["pubtype"]


def parse_title(x):
    e, md = x
    if e.attrib["type"] == "item":
        md["title"] = e.text
    if e.attrib["type"] == "source":
        md["publication"] = e.text


def parse_id(x):
    e, md = x
    md["id"] = e.text


def parse_doi(x):
    e, md = x
    if e.attrib["type"] == "doi":
        md["doi"] = e.attrib["value"]


def parse_abstract(x):
    e, md = x
    md["abstract"] = e.text


def parse_keywords(x):
    e, md = x
    if "keywords" not in md:
        md["keywords"] = []
    md["keywords"].append(e.text)


def parse_authors(x):
    e, md = x
    if "authors" not in md:
        md["authors"] = []
    md["authors"].append(e.text)


def parse_citations(x):
    e, md = x
    if "citations" not in md:
        md["citations"] = []
    md["citations"].append(e.text)


def is_wos(entry_id):
    return entry_id[:4] == "WOS:"


def filter_wos_only(entry):
    if not is_wos(entry["id"]):
        return None
    entry["citations"] = list(filter(is_wos, entry["citations"]))
    return entry


def has_citations(entry):
    return len(entry["citations"]) > 0


def sample_edges(threshold, entry):
    entry["citations"] = list(filter(lambda x: uniform(0, 1) < threshold, entry["citations"]))
    return entry

SD = "records/REC/static_data/"
SDS = SD + "summary/"
PARSERS = {"records/REC/UID": parse_id,
           "records/REC/dynamic_data/cluster_related/identifiers/identifier": parse_doi,
           SDS + "titles/title": parse_title,
           SDS + "pub_info": parse_pubinfo,
           SDS + "names/name/wos_standard": parse_authors,
           SD + "fullrecord_metadata/references/reference/uid": parse_citations,
           #SD + "fullrecord_metadata/abstracts/abstract/abstract_text/p": parse_abstract,
           SD + "item/keywords_plus/keyword": parse_keywords}


def stub_md():
    return {"citations": [],
            "authors": [],
            "keywords": [],
            "abstract": None,
            "doi": None,
            "id": None,
            "title": None,
            "publication": None,
            "date": None}


class WOSStream(ContentHandler):
    def __init__(self, stream, wos_only=False, sample_rate=None, must_cite=False, date_after=None):
        self.tree = ET.iterparse(stream, events=("start", "end"))
        self.path = []
        self.wos_only = wos_only
        self.sample_rate = sample_rate
        self.must_cite = must_cite
        if date_after and not isinstance(date_after, datetime.datetime):
            raise ValueError("date_after must be a datetime.datetime")
        self.date_after = date_after

    def parse(self):
        md = stub_md()
        for event, elem in self.tree:
            if event == "start":
                self.path.append(elem.tag[SKIP_LEN:])
            elif event == "end":
                elem.tag = elem.tag[SKIP_LEN:]
                parser = PARSERS.get("/".join(self.path))
                if parser:
                    parser((elem, md))

                self.path.pop()

                # TODO: These filters should really be elsewhere, wrapping the results of the parse call. Use Pipe?
                if elem.tag == "REC":
                    if self.date_after and md:
                        if not md["date"] or md["date"] < self.date_after:
                            md = None
                    if self.wos_only and md:
                        md = filter_wos_only(md)
                    if self.sample_rate and md:
                        md = sample_edges(self.sample_rate, md)
                    if self.must_cite and md and not has_citations(md):
                        md = None
                    if md:
                        yield md
                    md = stub_md()
                elem.clear()
