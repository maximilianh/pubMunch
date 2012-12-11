#!/usr/bin/env python
# python 2.5 default libraries
import sys, re, glob, urllib2, socket, os, doctest, \
    logging, subprocess, os.path, fcntl, time, collections

import orgDetect
# copyright 2011 Maximilian Haeussler maximilianh@gmail.com

# ======= CONSTANT/CONFIG =======

class Obj: # generic object, to store config
    pass
parameters = Obj()
conf = parameters

# matches for these are removed from the file (=replaced by spaces)
xmlTagsRe  = re.compile('<.*?>')     # an xml tag
mathTypeRe = re.compile('MathType@[^ ]*') # a mathtype formula

# for cleaning/splitting the text files into words
nonLetterRe= re.compile(r'[\W]') # any non-alphanumeric character
#digitRe    = re.compile('[0-9]')  # any digit
wordRe     = re.compile('[a-zA-Z]+') # any word

# for words consisting only of nucleodies
conf.MINNUCLLEN = 3 # minimum length of word to add to stack
conf.MINTRAILNUCLLEN = 17 # once string on stack is longer than this, MINNUCLLEN is ignored

# for non-nucleotide words
nonNucl       = re.compile("[^ACTGUactgu]") # regular expression that describes non-nucleotide letters (used for cleaning)
nuclRegex     = re.compile("[ACTGUactgu]")# regular expression that describes nucleotide letters
#MINWORDLEN    = int(t2gConfig.get("text","MinWordLen", 19)) # minimum length of word to be processed
#MINDNACONTENT = float(t2gConfig.get("text","MinDnaContent", 0.4)) # minimum content of nucleotide letters within a word
conf.MINWORDLEN    = 19
conf.MINDNACONTENT = 0.4

# for final filtering 
# minimum length of dna string to be output
conf.MINDNALEN  = conf.MINWORDLEN  
# maximum number of sequences per file to output
#MAXSEQS    = int(t2gConfig.get("text","maxSeqsPerDocument", 10000000)) 
conf.MAXSEQS     = 10000
# the minimum number of different letters in a word, to skip stuff like AAAAGGGGG
conf.MINDIFFLETTERS = 3 
# single uppercase words of this length that contain only ACTGU are accepted no
# matter what
conf.MINSHORTDNAWORD = 5

# only remove restriction sites from sequences shorter than this, longer seqs blat well enough
conf.RESTRCLEANMAXLEN = 30

# remove these restriction sites from start/end of <30bp sequences (allow 1 or 2 nucl before/after site)
conf.restrSites = [
"GAATTC"  , #EcoR1
"GATATC"  , #EcoRV
"CTCGAG"  , #XhoI
"TCTAGA"  , #XbaI
"GCTAGC"  , #NheI
"CCATGG"  , #NcoI
"GCGGCCGC", #NotI
"AGATCT"  , #BglII
"GGATCC"  , #BamH1
"AAGCTT"  , #HindIII
"GTCGAC"  , #SalI
"CCCGGG"  , #SmaI/XmaI
"ATCGAT"  , #Cla1
"TTTAAA"  , #Dra1
"CTGCAG"  , #Pst1
]

def compileRestrRes(conf):
    " generate start/end restr regex object from restriction sites "
    restrRes = []
    for reStr in conf.restrSites:
        flankNucl = "[ACTGUactgu]{0,2}" # restr enzymes cut better if they don't flank directly
        startRe = re.compile("^"+flankNucl+reStr)
        endRe = re.compile(reStr+flankNucl+"$")
        restrRes.append(startRe)
        restrRes.append(endRe)
    conf.restrRes = restrRes
    
compileRestrRes(conf)
# ==== SEQUENCE EXTRACTION =====

headers = ["start", "end", "seq", "partCount", "tainted"]
NucleotideOccurrence = collections.namedtuple("NucleotideOccurrence", headers)
## format of records that are returned by nucleotideOccurences:
#  - start, end = integers, position of occurence in text
#  - seq = a string, the sequence
#  - seqId = a number, starting from 0, for each sequence in a text
#  - partCount = integer, how many words did we have to join to obtain the sequence
#  - tainted = bool, were there any non-DNA letters in the sequence that we removed?

def replaceWithSpaces(regex, string):
    """ replaces all occurrences of regex in string with spaces 
    >>> replaceWithSpaces(xmlTagsRe, "<test> nonTag <test>")
    '       nonTag       '
    """
    def toSpaces(matchObject):
        return "".join([" "]*(matchObject.end(0) - matchObject.start(0)))
    return regex.sub(toSpaces, string) 

def removeOneRestrSite(seq):
    """ remove the first matching restr site from start or end of string 
    >>> removeOneRestrSite("CCCGGG")
    ''
    >>> removeOneRestrSite("AAAACCCGGGAAAA")
    'AAAACCCGGGAAAA'
    >>> removeOneRestrSite("ACTACTCCCGGGC")
    'ACTACT'
    >>> removeOneRestrSite("CGCCCGGGACTACT")
    'ACTACT'
    """
    global conf
    origSeq = seq
    for restrRe in conf.restrRes:
        if restrRe.search(seq)!=None:
            seq = restrRe.sub("", seq)
            return seq
    return seq

class MatchStack:
    """ a stack of matches to nucleotide regex objects in a text """
    def __init__(self):
        self.reset()

    def cleanPush(self, match):
        """ clean a word from non-nucleotides, then push onto stack"""
        self.tainted=True
        self.push(match, clean=True)

    def push(self, match, clean=False):
        """ append a word to the stack """
        newString = match.group(0)

        if clean:
            newString = nonNucl.sub("", newString) # remove all non-nucleotide characters

        newString = newString.replace("u", "t").replace("U", "T") # RNA -> DNA
        logging.log(5, "stack push: %s" % newString)
        self.strings.append(newString)
        self.charLen += len(newString)

        self.start = min(self.start, match.start())
        self.end   = max(self.end, match.end())

    def diffLetters(self):
        """ count the number of diff letters on the stack """
        longString = "".join(self.strings)
        longString = longString.lower()
        return len(set(longString))
        
    def seqLongEnough(self):
        diffLetters = self.diffLetters()
        return ( (self.charLen >= conf.MINDNALEN and \
                diffLetters >= conf.MINDIFFLETTERS)) \
            or (diffLetters > 1 and \
                self.isAllUpcaseOneWord() and \
                self.charLen>=conf.MINSHORTDNAWORD)

    def isAllUpcaseOneWord(self):
        """ return True if stack contains one uppercase word """
        if len(self.strings)!=1:
            return False
        stackWord = self.strings[0]
        return stackWord==stackWord.upper()
        
    def getOcc(self, text):
        """ return named tuple with the data of the current stack content 
        """
        nuclString         = "".join(self.strings)
        logging.log(5, "generating sequence from stack: sequence %s" % (nuclString))

        if len(nuclString) < conf.RESTRCLEANMAXLEN:
            nuclString         = removeOneRestrSite(nuclString)

        partCount  = len(self.strings)
        return NucleotideOccurrence(self.start, self.end, nuclString, partCount, self.tainted)

    def reset(self):
        logging.log(5, "stack reset")
        self.strings = []
        self.start   = 999999999 # start pos of first element
        self.end     = 0         # end position of last element
        self.charLen = 0         # length of whole stack in characters
        self.tainted = False     # has ASCII-cleaning been used to populate any element in the stack?


def cleanText(text):
    # clean: xml tags and mathtype -> spaces
    cleanText = replaceWithSpaces(xmlTagsRe, text)
    cleanText = replaceWithSpaces(mathTypeRe, text)
    # clean: non-letters -> spaces
    cleanText = nonLetterRe.sub(" ", cleanText)
    #cleanText = digitRe.sub(" ", cleanText) # XX removed NOv 18 2011
    return cleanText

def nucleotideOccurrences(text):
    """ Parse out all nucleotide-like strings from xml/ascii text and return them as a list of nucleotide occurence-records
    Example:

    >>> nucleotideOccurrences("test test caccatgacacactgacacatgtgtactgtg")[0]
    NucleotideOccurrence(start=10, end=41, seq='caccatgacacactgacacatgtgtactgtg', partCount=1, tainted=False)
    >>> nucleotideOccurrences("test test tga tga cac atg tgt act gtg a")[0].seq
    'tgatgacacatgtgtactgtga'
    >>> nucleotideOccurrences("bla bla actg ttt tcactybaactbacbatactbatcgactgactgactgtactcctacgatgcgtactacttacghhh")[0].seq
    'actgttttcactaactacatactatcgactgactgactgtactcctacgatgcgtactacttacg'

    """

    textClean = cleanText(text)

    stack = MatchStack()
    occurences = []

    for wordMatch in wordRe.finditer(textClean):
        word = wordMatch.group(0)
        logging.log(5, "word: %s" % word)

        dnaContent = float(len(nuclRegex.findall(word))) / len(word)

        if dnaContent==1.0 and \
                   (len(word)>=conf.MINNUCLLEN \
                or stack.charLen>conf.MINTRAILNUCLLEN \
                or (word==word.upper() and len(word)>=conf.MINSHORTDNAWORD)):
            # word is only nucleotides +
            # - long enough or 
            # - already enough chars in stack or
            # - longer than a minimum size and all upcase
            stack.push(wordMatch)
            continue
        elif len(word) >= conf.MINWORDLEN and dnaContent >= conf.MINDNACONTENT:
            # long words with enough DNA in them
            stack.cleanPush(wordMatch)
        else:
            # anything else triggers a stack output and reset
            if stack.seqLongEnough():
                occurences.append(stack.getOcc(textClean))
            stack.reset()

    # if document finishes with a non-empty stack: empty it
    if stack.seqLongEnough():
        occurences.append(stack.getOcc(textClean))

    if len(occurences) > conf.MAXSEQS:
        logging.log(5, "too many sequences in paper, skipping whole document")
        return []
    else:
        return occurences

""" interface to pubtools """
class Annotate:
    def __init__(self):
        self.headers = ["start", "end", "seq", "partCount", "tainted", "dbs"]
        self.orgDetect = orgDetect.OrgDetect()

    def annotateFile(self, articleData, fileData):
        """ interface for pubRun to get annotation lines for text """
        # find organisms in text
        dbs = set()
        text = fileData.content
        for row in self.orgDetect.annotRows(text):
            dbs.add(row[-1])
        dbString = ",".join(dbs)

        # find dna in text and add organisms
        for row in nucleotideOccurrences(text):
            if row.seq=="": # can only happen if seq is a restriction site
                continue
            row = [str(x) for x in row]
            row.append(dbString)
            yield row

# ----- 
if __name__ == "__main__":
    import doctest
    doctest.testmod()

