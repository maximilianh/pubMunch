# a few simple functions to split/clean/process text into sections and clean sentences

import re, logging, gzip, array, operator, orderedDict
from os.path import join
import pubGeneric, pubConf

# GLOBALS

# - for sentSplitter:
# adapted from 
# http://stackoverflow.com/questions/25735644/python-regex-for-splitting-text-into-sentences-sentence-tokenizing
sentSplitRe = re.compile(r'(?<!\w\.\w.)(?<!no\.)(?<![A-Z][a-z]\.)(?<![A-Z]\.)(?<!F[iI][gG]\.)(?<! al\.)(?<=\.|\?|\!|:)\s+')

# - for wordSplitter
# _ is considered part of word as it's mostly used for internal processing
# codes in html/pdf/xml and we want to keep these intact
wordRe = re.compile("[\w_]+", re.LOCALE)

#def sentSplitDumb(text):
    #""" split a string into sentences, returns a list of strings. """
    #return sentSplitRe.split(text)

# FUNCTIONS 

def wordSplitter(text):
    """ trivial splitting of text into words using a regex, yields start, end, word 
    """
    for match in wordRe.finditer(text):
        word = match.group()
        start, end = match.start(), match.end()
        yield start, end, word

def wordSet(text):
    """ returns the set of words in a string, all are lower-cased. 
    Underscore is considered part of words, as it's not used in science text.
    >>> sorted(wordSet("Red is my favorite. Blue-green are great. Red_blue not."))
    ['are', 'blue', 'favorite', 'great', 'green', 'is', 'my', 'not', 'red', 'red_blue']
    """
    text = text.lower()
    words = set(wordRe.findall(text))
    return words

def sentSplitter(text):
    """
    split a text into sentences, yield tuples of (start, end, sentence)

    >>> text = "Mr. Smith bought cheapsite.com for 1.5 million dollars, i.e. he paid a lot for it.  Come here, dog no. 5! Did he mind? Adam Jones Jr. thinks he didn't. In any case, this isn't true... Well, with a probability of .9 it isn't. The U.S.A. is a big country. C. elegans is tricky on Fig. 5 so is B. subtilis. Kent et al. is just a reference."
    >>> list(sentSplitter(text))
    [(0, 82, 'Mr. Smith bought cheapsite.com for 1.5 million dollars, i.e. he paid a lot for it.'), (84, 105, 'Come here, dog no. 5!'), (106, 118, 'Did he mind?'), (119, 151, "Adam Jones Jr. thinks he didn't."), (152, 183, "In any case, this isn't true..."), (184, 224, "Well, with a probability of .9 it isn't."), (225, 253, 'The U.S.A. is a big country.'), (254, 303, 'C. elegans is tricky on Fig. 5 so is B. subtilis.'), (304, 336, 'Kent et al. is just a reference.')]
    """
    lastStart = 0
    for match in sentSplitRe.finditer(text):
        newStart = match.start()
        yield lastStart, newStart, text[lastStart:newStart]
        lastStart = match.end()
    yield lastStart, len(text), text[lastStart:len(text)]

# SECTIONING OF TEXT

# main function: sectionSplitter
# - based on keywords that have to appear at the beginning of lines
# - additional filtering to ignore the pseudo-sections in structured abstracts
# - reference section uses a completely different approach based on clusters 
#   of family names
# sub functions: sectionRangesKeyword and findRefSection

# examples of articles where this DOES NOT work:
#  unzip /hive/data/outside/literature/ElsevierConsyn/2-00290-FULL-XML.ZIP 0140-6736/S0140673600X41633/S0140673686920064/S0140673686920064.xml - raw text, no linebreaks (but dots)

# regular expressions more or less copied from Ruihua Fang, based on her textpresso code
# see BMC http://www.biomedcentral.com/1471-2105/13/16/abstract
# XX could be improved by looking at pubmedCentral section names in XML
sectionResText = (
    ('abstract' ,r"(abstract|summary)"),
    ('intro', r"(introduction|background)"),
    ('methods', r"(materials?\s*and\s*methods|patients and methods|methods|experimental\s*procedures|experimental\s*methods)"),
    ('results', r"(results|case report|results\s*and\s*discussion|results\/discussion|figures and table|figures|tables)"),
    ('discussion', r"discussion"),
    ('conclusions', r"(conclusion|conclusions|concluding\s*remarks)"),
    ('ack', r"(acknowledgment|acknowledgments|acknowledgement|acknowledgements)"),
    ('refs', r"(literature\s*cited|references|bibliography|refereces|references\s*and\s*notes)")
)

# compile the regexes
#prefix = r"^[\s\d.IVX]*"
#suffix = r"\s*($|:)"
# html2text formats headers like this: ***** ABSTRACT *****
prefix = r"^[*\s\d.IVX]*"
suffix = r"\s*[*]*\s*($|:)"
flags = re.IGNORECASE | re.UNICODE | re.MULTILINE
sectionRes = [(name, re.compile(prefix+pat+suffix, flags)) for (name,pat) in sectionResText]

def sectionRangesKeyword(text):
    """
    split text into  sections 'header', 'abstract', 'intro', 'results', 'discussion', 'methods', 'ack', 'refs', 'conclusions', 'footer'
    return as ordered dictionary sectionName -> (start, end) tuple
    >>> sectionRangesKeyword("Introduction\\nResults\\n\\nReferences\\nNothing\\nAcknowledgements")
    OrderedDict([('header', (0, 0)), ('intro', (0, 13)), ('results', (13, 21)), ('refs', (21, 41)), ('ack', (41, 57))])
    >>> text = "hihihi sflkjdf\\n Results and Discussion\\nbla bla bla\\nI. Methods\\n123. Bibliography\\n haha ahahah ahah test test\\n"
    >>> sectionRangesKeyword(text)
    OrderedDict([('header', (0, 15)), ('results', (15, 51)), ('methods', (51, 62)), ('refs', (62, 108))])
    """
    text = text.replace("\a", "\n")
    # get start pos of section headers, create list ('header',0), ('discussion', 400), etc
    sectionStarts = []
    for section, regex in sectionRes:
        #logging.log(5, "Looking for %s" % section)
        for match in regex.finditer(text):
            #logging.log(5, "Found at %d" % match.start())
            sectionStarts.append((section, match.start()))
    sectionStarts.sort(key=operator.itemgetter(1))
    sectionStarts.insert(0, ('header', 0))
    sectionStarts.append((None, len(text)))

    # convert to dict of starts for section
    # create dict like {'discussion' : [200, 500, 300]}
    sectionStartDict = orderedDict.OrderedDict()
    for section, secStart in sectionStarts:
        sectionStartDict.setdefault(section, [])
        sectionStartDict[section].append( secStart )

    if len(sectionStartDict)-2<2:
        logging.log(5, "Fewer than 2 sections, found %s, aborting sectioning" % sectionStarts)
        return None

    # convert to list with section -> (best start)
    bestSecStarts = []
    for section, starts in sectionStartDict.iteritems():
        if len(starts)>2:
            logging.log(5, "Section %s appears more than twice, aborting sectioning" % section)
            return None
        if len(starts)>1:
            logging.log(5, "Section %s appears more than once, using only second instance" % section)
            startIdx = 1
        else:
            startIdx = 0
        bestSecStarts.append( (section, starts[startIdx]) )
    logging.log(5, "best sec starts %s" % bestSecStarts)

    # skip sections that are not in order
    filtSecStarts = []
    lastStart = 0
    for section, start in bestSecStarts:
        if start >= lastStart:
            filtSecStarts.append( (section, start) )
            lastStart = start
    logging.log(5, "filtered sec starts %s" % filtSecStarts)

    # convert to dict with section -> start, end
    secRanges = orderedDict.OrderedDict()
    for i in range(0, len(filtSecStarts)-1):
        section, secStart = filtSecStarts[i]
        secEnd = filtSecStarts[i+1][1]
        secRanges[section] = (secStart, secEnd)

    # bail out if any section but [header, footer] is of unusual size
    maxSectSize = int(0.7*len(text))
    minSectSize = int(0.003*len(text))
    for section, secRange in secRanges.iteritems():
        if section=='header' or section=='footer':
            continue
        start, end = secRange
        secSize = end - start
        if secSize > maxSectSize:
            logging.debug("Section %s too long, aborting sectioning" % section)
            return None
        elif secSize < minSectSize and section not in ["abstract", "ack"]:
            logging.debug("Section %s too short, aborting sectioning" % section)
            return None
        else:
            pass

    logging.debug("Sectioning OK, found %s" % secRanges)
    return secRanges

famNames = None

def _readFamNames():
    " read family names into global var 'names', lowercase them "
    global famNames
    famNames = set()
    fname = join(pubConf.staticDataDir, "famNames", "commonNames.txt")
    logging.info("Reading family names from %s for reference finder" % fname)
    for l in open(fname):
        famNames.add(l.rstrip("\n").decode("utf8").lower())
    famNames.update(["references", "bibliography", "literature", "refereces"])
    logging.info("Loaded %d family names" % len(famNames))

def _skipForwMax(text, start, maxDist):
    """ skip forward to next linebreak from start to start+maxDist
    Return start if not found, otherwise position of linebreak 
    >>> _skipForwMax("hi there \\nis no linebreak", 3, 20)
    9
    """
    for i in range(start, min(start+maxDist, len(text))):
        if text[i] in ["\a", "\n", "\r"]:
            return i
    return start
        
def _findLongestRun(arr):
    """ find longest run of 1s in mask 
    >>> _findLongestRun([0,0,0,0,1,1,1,1,0,1,1,0,0,0,1,1]) 
    (4, 8)
    """
    size = 0
    maxSize = 0
    runStart = None
    bestStart = 0
    bestEnd = 0
    for i in range(0, len(arr)):
        if arr[i]==1:
            size += 1
            if runStart == None:
                runStart = i
        else:
            if size > maxSize:
                bestStart = runStart
                bestEnd = i
                maxSize = size
            size = 0
            runStart = None

    return bestStart, bestEnd
                
def findRefSection(text, nameExt=250):
    """ 
    Finds a single reference section in text, based on dense clusters of
    family names.

    Searches for longest stretch of family names separated by up to
    nameExt chars. Section must be longer than minLen and start in the 2nd half
    of the text.
    The first family name starts the section, the last family name is extended
    to the end of the line. Special names are references and bibliography that
    are not family names but are treated as such.
    
    Uses a strange bitmask-approach but that keeps the algorithm simple.
    Returns a tuple (start, end)
    >>> text = "                                                     some test text\\nmore test text\\nreferences\\nhaussler brian raney mueller a great paper\\nsome more text"
    >>> start, end = findRefSection(text, nameExt=5)
    >>> text[start:end]
    'references\\nhaussler brian raney mueller a great paper'
    """
    if famNames==None:
        # lazily read lowercased family names
        _readFamNames()

    mask = array.array("b", len(text)*[0])

    # go over words in document, if word is family name, extend by 250 chars on
    # both sides and add to mask
    for start, end, word in wordSplitter(text.lower()):
        if word not in famNames:
            continue
        logging.debug("Found name: %s, %d-%d" % (word, start, end))

        leftBox  = max(0, start-nameExt)
        rightBox = min(len(text), end+nameExt)

        for i in range(leftBox, rightBox):
            mask[i] = 1

    refStart, refEnd = _findLongestRun(mask)

    # refStart should be the start of the first author name, so take back extension
    refStart = refStart + nameExt
    # refEnd should be the end of the last author + some margin
    if refEnd-refStart > nameExt:
        refEnd = refEnd - (nameExt/2)
    # try to move refEnd up to the next linebreak
    refEnd = _skipForwMax(text, refEnd, 250)

    # ref section has to start in 2nd half of doc
    if refStart < len(text)/2:
        logging.debug("ignored: refs in 1st half of text")
    else:
        return refStart, refEnd
    return None, None

def _coordOverlap(start1, end1, start2, end2):
    """ returns true if two ranges overlap """
    result = (( start2 <= start1 and end2 > start1) or \
            (start2 < end1 and end2 >= end1) or \
            (start1 >= start2 and end1 <= end2) or \
            (start2 >= start1 and end2 <= end1))
    return result

def appendAndCut(start, end, secName, sections, refStart, refEnd):
    """ 
    add start-end to sections, cutting around refStart-refEnd 
    This can result in 0-length sections

    >>> appendAndCut(1, 30, "t", [], 5, 15) # included
    [(1, 5, 't'), (5, 15, 'refs'), (15, 30, 't')]
    >>> appendAndCut(1, 10, "t", [], 5, 15) # start overlap
    [(1, 5, 't'), (5, 15, 'refs')]
    >>> appendAndCut(1, 30, "t", [], 1, 15) # included
    [(1, 1, 't'), (1, 15, 'refs'), (15, 30, 't')]
    >>> appendAndCut(1, 30, "t", [], 1, 30) # included
    [(1, 1, 't'), (1, 30, 'refs')]
    >>> appendAndCut(1, 30, "t", [], 1, 30) # included
    [(1, 1, 't'), (1, 30, 'refs')]
    >>> appendAndCut(1, 30, "t", [], 15, 45) # overlap at end
    [(1, 15, 't'), (15, 45, 'refs')]
    """
    # possible overlaps:
    #  <---------1-------------><------->
    #       <------refs------->
    #  <---2---><---3----><-4--><---5--->
    # case 1: section includes refs -> split section
    if start <= refStart and end > refEnd:
        sections.append((start, refStart, secName))
        sections.append((refStart, refEnd, "refs"))
        sections.append((refEnd, end, secName))
    # case 2: section overlaps refStart -> trim section at front
    elif start <= refStart and end > refStart:
        sections.append((start, refStart, secName))
        sections.append((refStart, refEnd, "refs"))
    # case 3: section is included in refs -> skip
    elif start <= refStart and end < refEnd:
        return sections
    # case 4: section overlaps refEnd -> trim section at end
    elif start < refEnd and end >= refEnd:
        # no need to add the refs section, has been done before
        sections.append((refEnd, end, secName))
    # case 5: no overlap -> just add
    else:
        sections.append((start, end, secName))
    return sections

def sectionSplitter(text, fileType, refMinLen=500):
    """ 
    split file into sections. yields tuples with (start, end, section) 
    Based on keywords that have to appear at the beginning of lines
    Additional filtering to ignore the pseudo-sections in structured abstracts
    Reference section uses a completely different approach based on clusters 
    of family names
    Used like this:
    sections = sectionSplitter(text, fileData.fileType)

    Yields tuples (start, end, section)
    section is one of:
    'abstract','intro','methods','results','discussion','conclusions','ack','refs'
    or 'unknown' or 'supplement'
    """
    # find sections based on keywords on lines
    if fileType=="supp":
        sections = {"supplement": (0, len(text))}
    else:
        sections = sectionRangesKeyword(text)
        if sections==None:
            sections = {"unknown": (0, len(text))}

    refStart, refEnd = findRefSection(text)
    newSections = []
    if refStart==None or refEnd-refStart < refMinLen:
        # if ref section not found/too short: 
        # just return the keyword-based sections as they are
        for section, startEnd in sections.iteritems():
            start, end = startEnd
            newSections.append( (start, end, section))
    else:
        # merge ref section into existing sections
        # We need to trim the other, overlapping sections, so
        # they don't overlap the clustering-based ref section.
        for secName, secCoords in sections.iteritems():
            if secName=="refs":
                continue
            start, end = secCoords
            newSections = appendAndCut(start, end, secName, newSections, refStart, refEnd)
        # remove any empty sections
        newSections = [(start,end,name) for start,end,name in newSections if end-start!=0]

    return newSections

# --- frequently used English words -----

commonWords = None

def initCommonWords():
    """ read BNC all o5 top1000 wordlist into memory
    >>> initCommonWords()
    >>> isCommonWord("my")
    True
    """
    global commonWords
    if commonWords!=None:
        return
    commonWords = set()
    fname = join(pubConf.staticDataDir, "bnc", "bncTop1000.txt")
    for line in open(fname):
        if "_" in line: # multiword expressions
            continue
        word = line.rstrip("\n")
        commonWords.add(word)

def isCommonWord(w):
    " this is somewhat slower than to use the commonWords set directly "
    return (w in commonWords)

if __name__ == "__main__":
   import doctest
   doctest.testmod()
 
