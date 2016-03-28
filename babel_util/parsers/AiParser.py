#!/usr/bin/env python
import logging
import ujson
import pprint

#def formate_date(paperDate):
	#return paperDate #eventually we will formate the date to meet our needs

def parse_authors(context, rest):
	if len(rest) == 0:
		logging.info("No authors found")
	authors = []
	for item in rest:
		for key in item.keys():
			if key == 'name':
				authors.append(item[key])
	context["authors"] = authors

def parse_date(context, rest):
	if rest == 0 or rest == None:
		logging.info("No date found: " + rest)
	context["date"] = rest

def parse_id(context, rest):
	if len(rest) == 0:
		logging.info("No index found: " + rest)
	context["id"] = rest

def parse_title(context, rest):
	if len(rest) == 0:
		logging.info("No title found: " + rest)
	context["title"] = rest

def parse_venue(context, rest):
	if len(rest) == 0:
		logging.info("No venue found: " + rest)
	context["venue"] = rest

def parse_org(context, rest):
	if len(rest) == 0:
		logging.info("No org found: " + rest)
	context["org"] = rest

def parse_name(context, rest):
	if len(rest) == 0:
		logging.info("No name found: " + rest)
	context["name"] = rest

def parse_citing_count(context, rest):
	if rest == 0 or rest == None:
		logging.info("No citation count found: " + rest)
	context["citing_count"] = rest

def parse_key_phrases(context, rest):
	if len(rest) == 0:
		logging.info("No key phrases found")
	context["keywords"] = rest

def parse_abstract(context, rest):
	if len(rest) == 0:
		logging.info("No abstract found: " + rest)
	context["abstract"] = rest

def parse_cited_by(context, rest):
	if len(rest) == 0:
		logging.info("No cited by ID's found")
	context["citedBy"] = rest

#use ws.py from parsers 
TOKEN_MAP = {
	"INIT_CONTEXT" : lambda: {  "citations": [],
								"citedBy": [],
								"authors": [],
								"keywords": [], 
								"abstract": None,
								"doi": None,
								"id": None,
								"title": None,
								"publication": None,
								"date": None, 
								"affiliations": None,
							 },
	"authors": parse_authors,
	"year": parse_date,
	"id": parse_id,
	"title": parse_title,
	"venue": parse_venue,
	"paperAbstract": parse_abstract, #maybe use bodyText
	"keyPhrases": parse_key_phrases,
	"citedBy": parse_cited_by,

	"org": None, #no org in the dataset
	"sourceInfo": None, #there is a doi URL in sourceInfo
	"journal": None #can this be used for publication? it has a weird format though

}

class AiParser(object):

	def __init__(self, token_map=TOKEN_MAP):
		self.token_map = token_map
		self.init_context()
		return

	def init_context(self):
		self.context = self.token_map["INIT_CONTEXT"]()

	def parse(self, stream):
		for entry in stream:
			orig_dict = ujson.loads(entry)
			for key in orig_dict.keys():
				if key in self.context.keys():
					self.token_map[key](self.context, orig_dict[key])

			yield self.context
			self.init_context()


			

if __name__ == "__main__":
	import argparse
	import sys
	parser = argparse.ArgumentParser()
	parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin)
	parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout)
	args = parser.parse_args()

	"""p = AiParser()
	count = 0
	for paper in p.parse(args.infile):
		count = count + 1
		print count
		print pprint.pprint(paper)
		args.outfile.write(ujson.dumps(paper))"""

	p = AiParser()
	for paper in p.parse(args.infile):
		args.outfile.write(ujson.dumps(paper))



"""
'part-00799'
bodyText
clusterInfo
classification
citedByYearHistogram
numCitedBy
followOn
year
id
citationContexts
dataSetNames
paperAbstract
title
citedBy
keyPhrases
journal
numCiting
indexInclusionReason
authors
citationVelocity
numKeyReferences
venue
sourceInfo
numKeyCitations
"""










