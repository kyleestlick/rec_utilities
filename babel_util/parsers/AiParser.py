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

def parse_publication(context, rest):
	if len(rest) == 0:
		logging.info("No org found: " + rest)
	context["publication"] = rest

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
								"venue": None
							 },
	"authors": parse_authors,
	"year": parse_date,
	"id": parse_id,
	"title": parse_title,
	"venue": parse_venue,
	"paperAbstract": parse_abstract, 
	"keyPhrases": parse_key_phrases,
	"citedBy": parse_cited_by,
	"journal": parse_publication

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


	p = AiParser()
	for paper in p.parse(args.infile):
		args.outfile.write(ujson.dumps(paper))
		args.outfile.write('\n')












