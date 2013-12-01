#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Author: Brian Lin
'''
Finds sentences from a file that mention two proteins and an interaction. Outputs sentences delimited by newlines in a file and associated information (provenance, expected genes, etc.) into another file. Requires a working version of geneFinder.py.

Cool things attempted:
- pubGeneric.py's section splitter: I wasn't able to successfully split any papers so I took this out.
- Cutting words from sentences to the left and right of the area encompassed by the genes and interactions: not a good idea actually, we don't want to cut out information useful for NLP
- Remove inline citations such as [17,18]: regular expression too slow, especially on matrices

'''
import logging
import sys
import argparse
import datetime
try:
	import re2 as re
except ImportError:
	import re
import geneFinder
import pickle
import marshal
import pubGeneric
import gzip
from collections import defaultdict
from unidecode import unidecode

# Temporary variables for DEBUGGING only! Tells Max which symbols are being found by geneFinder but not listed in entrez2sym.
nonamed = set()
def main():
	'''
	general gist: loads necessary information, then calls parseLines() which returns a valid sentence to be parsed, valid meaning we are going to throw it in TEES or other NLP program.
	'''
	args = parseArgs()
	logging.info("Parsed arguments")
	geneFinder.initData(exclMarkerTypes=["dnaSeq"]) # setup for findGenes() later
	logging.info("Set up environment for findGenes()")
	logging.info("Load pickled/marshaled relex, authors, entrez symbols")
	relex = pickle.load(args.extractionData)
	authors = pickle.load(args.extractionData)
	entrez = marshal.load(args.entrezData)['entrez2sym']
	logging.info("Open output files")
	sentenceFile, geneFile = open(args.timestamp+'-sentence.txt', 'w'), open(args.timestamp+'-genes.txt', 'w')
	for pmid, sentence, geneIds, geneNames, rawNames, relations in parseLines(args.inputFiles, entrez, relex, authors):
		geneFile.write(formatMeta(pmid, geneIds, geneNames, rawNames, relations))
		print formatMeta(pmid, geneIds, geneNames, rawNames, relations)
		sentenceFile.write(sentence + '\n')
		geneFile.flush()
	if len(nonamed) > 1:
		logging.warning("Not in entrez2sym: {}".format(nonamed))

def parseLines(input, entrez, relex, authors):
	'''
	iterates through valid sentences, valid meaning the sentence is a good candidate for being parsed. Uses geneFinder() to find genes and positions of genes within the sentence, then extracts the metainfo we need using extractGenes(). At this step, filter out sentences without enough genes or relations (aka no possible interactions). Also filter out references using an authors database. It's important to process all Unicode characters because not all programs can handle them.
	'''
	for pmid, sentence in parseSentences(input):
		# logging.info("Parsing line: {}".format(sentence[:30] + ' ...... '))
		try:
			decodedSentence = unidecode(sentence.decode('utf-8'))
		except UnicodeDecodeError:
			logging.warning("Can't process Unicode for {}: {}".format(pmid, sentence))
			decodedSentence = sentence
		if isReference(sentence, authors):
			continue
		genesSupport, _ = geneFinder.findGenes(decodedSentence)
		geneIds, geneNames, rawNames = extractGenes(genesSupport, entrez, decodedSentence)
		relations = findRelations(sentence, relex)
		if len(geneNames) < 2 or len(relations) == 0:
			continue
		yield pmid, decodedSentence, geneIds, geneNames, rawNames, relations

def parseSentences(inputFiles):
	'''
	parse sentences given input file handles. We want to iterate through all rows of all input tab-delimited full text collections. Filter a paper if it has nothing to do with interactions, or if the file is in the wrong format. Return sentence back up to parseLines() to find genes in it.
	'''
	for inputFile in openFiles(inputFiles):
		logging.info('Processing tab-delimited full text rows in {}'.format(inputFile.name))
		for line in inputFile:
			fileId, pmid, _, _, desc, fileType, _, _, _, text = line.split('\t')
			if not isBioPaper(text) or re.search('pdf', desc) or fileType != 'main' or pmid == '':
				logging.info('FileID {} is not a valid biomedical text file'.format(fileId))
				continue
			logging.info('Processing fileId {}'.format(fileId))
			for sentence in processText(text):
				yield pmid, sentence

def isBioPaper(text):
	if ' gene ' in text or ' protein ' in text:
		return True
	return False
		

def processText(text):
	'''
	strips some unwanted characters. Originally stripped the "references" section according to pubGeneric but it wasn't working. Splits full text strings by a simple sentence filter.
	'''
	text = re.sub(r'\x07|\r', '', text)
	#text = re.sub(r'\x07|\r|[(\s{0,3}\d{1,3}\s{0,3})(,\s{0,3}\d{1,3}\s{0,3}){0,7}\]', '', text)
		# strip ^G, \r, and inline citations
	#sections = pubGeneric.sectionRanges(text)
	#if sections is not None:
	#	try:
	#		dropRange = sections['ack']
	#		text = text[:dropRange[0]] + text[dropRange[1]:]
	#	except KeyError:
	#		pass
	#	try:
	#		dropRange = sections['refs']
	#		text = text[:dropRange[0]] + text[dropRange[1]:]
	#	except KeyError:
	#		pass
	
	# split by period followed by capital letter within 3 proceeding characters
	previousThreshold = -2
	threshold = 0
	for threshold in re.finditer('\..?.?([A-Z])', text):
		threshold = threshold.start()
		yield text[previousThreshold+2:threshold+1]
		previousThreshold = threshold
	yield text[threshold:]

def isReference(sentence, authors):
	''' checks if a sentence is a reference - use a hashed author database to check each word'''
	score = 0
	for word in re.split('[ ,;]', sentence):
		if word in authors:
			score += 1
		if score > 3:
			return True
	return False

def extractGenes(genesSupport, entrez, sentence):
	'''
	given gene IDs and their positions, grabs their official names according to entrez2sym and extracts the raw names geneFinder flagged. This is so we can manually inspect the -genes.txt file for false positives (and if we need anything else later, which is likely). 
	'''
	# first - get a list of all the gene IDs
	geneIds, genePositions = [], []
	for geneId in genesSupport:
		entry = genesSupport[geneId]
		try:
			listedId, genePositionList = entry['geneName'][0]
			if listedId not in geneIds:
				geneIds.append(listedId)
				genePositions.append(genePositionList[0])
		except:
			continue
	# match up gene IDs to official names. Sometimes geneFinder will yield something like 6362/6368/6359 and we need to process all of them. In that case it is CCL18/CCL23/CCL15 and the raw gene name is 'macrophage inflammatory protein'.
	geneNames = []
	for geneId in geneIds:
		splitGenes = geneId.split('/')
		if len(splitGenes) > 1:
			geneName = []
			for splitId in splitGenes:
				if int(splitId) in entrez:
					geneName.append(entrez[int(splitId)])
				else:
					logging.info('Gene ID not in entrez2sym for {} in: '.format(splitId, sentence))
					nonamed.add(splitId)
			geneNames.append('/'.join(geneName))
		else:
			if int(geneId) in entrez:
				geneName = entrez[int(geneId)]
				geneNames.append(geneName)
			else:
				logging.info('Gene ID not in entrez2sym for {} in: '.format(geneId, sentence))
				nonamed.add(int(geneId))
	# lastly, extract raw gene names as seen in the text
	rawNames = []
	for position in genePositions:
		rawNames.append(sentence[position[0]:position[1]])
	return geneIds, geneNames, rawNames

def findRelations(sentence, relex):
	''' iterate through words in a sentence and extract all relations '''
	relations = set()
	for word in filter(None, re.split('[ ,.]', sentence)):
		if word in relex:
			relations.add(word)
	return relations

def formatMeta(pmid, geneIds, geneNames, rawNames, relations):
	string = '{}\t{}\t{}\t{}\t{}'.format(pmid, '|'.join(geneIds), '|'.join(geneNames), '|'.join(rawNames), '|'.join(relations))
	string = unidecode(string) # just in case
	return string

def parseArgs():
	parser = argparse.ArgumentParser()
	parser.add_argument('-v', '--verbose', action='store_true', help='Show debug messages') 
	parser.add_argument('inputFiles', nargs='+', help='Input text file(s), will also process gzip files')
	parser.add_argument('-p', '--extractionData', type=argparse.FileType('r'), default='/cluster/home/blin/hive/mining/text_filter/extractionData.pkl', help='Pickled file with relation and author dictionaries')
	parser.add_argument('-e', '--entrezData', type=argparse.FileType('r'), default='/hive/data/inside/pubs/geneData/entrez.9606.tab.marshal', help='Marshaled entrez-symbol dictionary')
	args = parser.parse_args()
	args.timestamp = 'run-'+datetime.datetime.today().strftime("%a-%b-%d-%Y-%H%M%S")
	if args.verbose:
		logging.basicConfig(level=logging.INFO)
	else:
		logging.basicConfig(filename=args.timestamp+'.log', filemode='w', level=logging.INFO)
	return args


def openFiles(inputFiles):
	''' Creates file handles from input filename strings. This is due to resource.RLIMIT_NOFILE restricting the number of file handles open, so we are going to yield and close instead. Contains additional gzip handling '''
	for filename in inputFiles:
		inputFile = gzip.GzipFile(filename, 'r') if filename.endswith('gz') else open(filename, 'r')
		yield inputFile
		inputFile.close()

if __name__ == '__main__':
	main()

