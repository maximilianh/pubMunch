# a few simple functions to split/clean/process text into sections and clean sentences

# testing code is part of the function comments (python doctests)

#>>> logger = logging.getLogger().setLevel(5)
#>>> logger = logging.getLogger().setLevel(logging.INFO)

import re, logging, gzip, array, operator, orderedDict
from os.path import join
import pubGeneric, pubConf
import unidecode

# GLOBALS

# - for sentSplitter:
# adapted from 
# http://stackoverflow.com/questions/25735644/python-regex-for-splitting-text-into-sentences-sentence-tokenizing
sentSplitRe = re.compile(r'(?<!\w\.\w.)(?<!no\.)(?<![A-Z][a-z]\.)(?<![A-Z]\.)(?<!F[iI][gG]\.)(?<! al\.)(?<=\.|\?|\!|:)\s+')

# - for wordSplitter
# _ is considered part of word as it's mostly used for internal processing
# codes in html/pdf/xml and we keep these intact so we can remove them
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

# ------ SECTIONING OF TEXT

# minimum length of section, shorter sections will be skipped
MINSECLEN = 400

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
# format is: sectionName, startRatio, endRatio, regex-pattern
# match has to be located between startRatio-endRatio of total text len
sectionResText = (
    ('abstract' , 0.0, 0.2, r"(abstract|summary)"),
    ('intro', 0.01, 0.5, r"(introduction|background)"),
    ('methods', 0.01, 0.9, r"(materials?\s*and\s*methods|patients and methods|methods|experimental\s*procedures|experimental\s*methods)"),
    ('results', 0.1, 0.95, r"(results|case report|results\s*and\s*discussion|results\/discussion|figures and table|figures|tables)"),
    ('discussion', 0.01, 0.90, r"discussion"),
    ('conclusions', 0.1, 0.9, r"(conclusion|conclusions|concluding\s*remarks)"),
    ('notes', 0.8, 1.0, "(footnotes)"),
    ('refs', 0.5, 1.0, r"(literature\s*cited|references|bibliography|refereces|references\s*and\s*notes)"),
    ('ack', 0.5, 1.0, r"(acknowledgment|acknowledgments|acknowledgement|acknowledgements)")
)

shortSections = ["abstract", "ack", "notes"]

# compile the regexes
# html2text formats headers like this: ***** ABSTRACT *****
prefix = r"^[*\s\d.IVX]*"
suffix = r"[\s*:]*$"
flags = re.IGNORECASE | re.UNICODE | re.MULTILINE
sectionRes = [(name, start, end, re.compile(prefix+pat+suffix, flags)) for (name,start, end, pat) in sectionResText]

def removeCloserThan(namePosList, minDist, exceptList=shortSections):
    """
    given a sorted list of (name, pos) tuples, remove all elements that are 
    closer than minDist to either the preceding or the following element.
    The idea is to remove matches to keywords that are too close together, like
    in tables of contents or other lists that we are not interested in.
    >>> removeCloserThan([("a", 1000), ("b", 2000), ("c", 2007), ("d",2100)], 200)
    [('a', 1000)]
    >>> removeCloserThan([("a", 1), ("b", 2), ("c", 3000)], 200, exceptList=['b'])
    [('b', 2), ('c', 3000)]
    """
    lastPos, lastName = 0, None
    newList = []
    for i in range(0, len(namePosList)):
        name, pos = namePosList[i]
        # compare with preceding
        if i>0 and pos-lastPos <= minDist and not name in exceptList:
            logging.log(5, "too close to preceding, %s, %d" % (name, pos))
            lastPos, lastName = pos, name
            continue
        # compare with next
        elif i<len(namePosList)-1:
            nextName, nextPos = namePosList[i+1]
            if nextPos-pos <= minDist and not name in exceptList:
                logging.log(5, "too close to next, %s, %d" % (name, pos))
                lastPos, lastName = pos, name
                continue

        lastPos, lastName = pos, name
        newList.append( (name, pos) )

    return newList

def sectionRangesKeyword(text, minDist=MINSECLEN, doRefs=True):
    """
    split text into  sections 'header', 'abstract', 'intro', 'results',
    'discussion', 'methods', 'ack', 'refs', 'conclusions', 'footer'.
    'refs' is only added if "doRefs" is True.

    return a list of (start, end, name) 
    >>> sectionRangesKeyword("some text\\nIntroduction\\nResults\\n\\nNothing\\nAcknowledgements\\nReferences\\nRef text", minDist=1)
    [(0, 10, 'header'), (10, 23, 'intro'), (23, 40, 'results'), (40, 57, 'ack'), (57, 76, 'refs')]
    >>> text = "hihihi sflkjdf\\n Results and Discussion\\nbla bla results results results bla\\nI. Methods\\n123. Bibliography\\n haha ahahah ahah test test\\n"
    >>> sectionRangesKeyword(text, minDist=5)
    [(0, 15, 'header'), (15, 75, 'results'), (75, 86, 'methods'), (86, 132, 'refs')]

    >>> text = "headers\\nIntroduction\\nResults\\n***** Discussion *****\\nbla bla bla bla bla blab bla bla\\n"
    >>> sectionRangesKeyword(text, minDist=1)
    [(0, 8, 'header'), (8, 21, 'intro'), (21, 29, 'results'), (29, 85, 'discussion')]

    """
    text = text.replace("\a", "\n")
    # get start pos of section headers, create list ('header',0),
    # ('discussion', 400), etc
    # 'abstract' appears in html files very often, so only use the first match
    sectionStarts = []
    absCount = 0
    for section, start, end, regex in sectionRes:
        #logging.log(5, "Looking for %s" % section)
        if not doRefs and section=="refs":
            continue
        for match in regex.finditer(text):
            pos = match.start()
            logging.log(5, "Section %s found at %d" % (section, pos))
            logging.log(5, "excerpt: %s" % text[pos-100:pos+100])
            if pos < start*len(text):
                logging.log(5, "%s at %d is before start limit" % (section, pos))
                continue
            if pos > end*len(text):
                logging.log(5, "%s at %d is after end limit" % (section, pos))
                continue
            #print match.start(), section
            if section=="abstract":
                absCount += 1
                if absCount>1:
                    break
            sectionStarts.append((section, pos))

    sectionStarts.sort(key=operator.itemgetter(1))
    logging.log(5, "sectioning phase 1: "+repr(sectionStarts))

    sectionStarts = removeCloserThan(sectionStarts, minDist)
    sectionStarts.insert(0, ('header', 0))
    sectionStarts.append((None, len(text)))
    logging.log(5, "sectioning phase 2: "+repr(sectionStarts))

    # convert to dict of starts for section
    # create dict like {'discussion' : [200, 500, 300]}
    sectionStartDict = orderedDict.OrderedDict()
    for section, secStart in sectionStarts:
        sectionStartDict.setdefault(section, [])
        sectionStartDict[section].append( secStart )

    if len(sectionStartDict)-2<2: # don't count start and end markers as sections
        logging.log(5, "Fewer than 2 sections, found %s, aborting sectioning" % sectionStarts)
        return None

    # convert to list with section -> (best start)
    bestSecStarts = []
    for section, starts in sectionStartDict.iteritems():
        if len(starts)>2:
            logging.log(5, "Section %s appears more than twice, aborting sectioning" % section)
            return None
        # handle structured abstracts
        #if len(starts)>1:
            #logging.log(5, "Section %s appears more than once, using only second instance" % section)
            #startIdx = 1
        #else:
        # startIdx = 0
        bestSecStarts.append( (section, starts[0]) )
    logging.log(5, "best sec starts %s" % bestSecStarts)

    # skip sections that are not in order
    filtSecStarts = []
    lastStart = 0
    for section, start in bestSecStarts:
        if start >= lastStart:
            filtSecStarts.append( (section, start) )
            lastStart = start
    logging.log(5, "removed overlaps %s" % filtSecStarts)

    # convert to list of (start, end, name)
    secRanges = []
    for i in range(0, len(filtSecStarts)-1):
        section, secStart = filtSecStarts[i]
        secEnd = filtSecStarts[i+1][1]
        secRanges.append( (secStart, secEnd, section) )
    # last element was "none" section dummy anyways

    # bail out if any section is of unusual size
    maxSectSize = int(0.7*len(text))
    minSectSize = int(0.003*len(text))
    for start, end, section in secRanges:
        if section=='header' or section=='footer':
            continue
        secSize = end - start
        if secSize > maxSectSize:
            logging.debug("Section %s too long, aborting sectioning" % section)
            return None
        elif secSize < minSectSize and section not in shortSections:
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
    # not family names, but useful to delimit the start of the reference 
    # section
    famNames.update(["references", "bibliography", "literature", "refereces"])
    logging.info("Loaded %d family names" % len(famNames))

def skipForwMax(text, start, maxDist):
    """ skip forward to next linebreak from start to start+maxDist
    Return start if not found, otherwise position of linebreak 
    >>> skipForwMax("hi there \\nis no linebreak", 3, 20)
    9
    >>> skipForwMax("hi there \\nis no linebreak", 3, 2)
    3
    """
    for i in range(start, min(start+maxDist, len(text))):
        if text[i] in ["\a", "\n", "\r"]:
            return i
    return start
        
def _findLongestRun(arr):
    """ find longest run of 1s in mask 
    >>> _findLongestRun([0,0,0,0,1,1,1,1,0,1,1,0,0,0,1,1]) 
    (4, 8)
    >>> _findLongestRun([0,0,0,0,1,1,1,1,0,1,1,1,1,1,1,1]) 
    (9, 15)
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
                if size > 1000:
                    logging.log(5, "name-run found at %d-%d" % (bestStart, bestEnd))
                maxSize = size
            size = 0
            runStart = None
    if size > maxSize:
        bestStart, bestEnd = runStart, i

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
        logging.log(5, "Found name: %s, %d-%d" % (word, start, end))

        leftBox  = max(0, start-nameExt)
        rightBox = min(len(text), end+nameExt)

        for i in range(leftBox, rightBox):
            mask[i] = 1

    refStart, refEnd = _findLongestRun(mask)
    logging.log(5, "reference section based on names: %d-%d" % (refStart, refEnd))
    logging.log(5, "excerpt: %s" % (text[refStart:refEnd]))

    # refStart should be the start of the first author name, so take back extension
    refStart = refStart + nameExt
    # refEnd should be the end of the last author + some margin
    #if refEnd-refStart > nameExt:
        #refEnd = refEnd - (nameExt/2)
    # try to extend to end of line and by two more lines
    refEnd = skipForwMax(text, refEnd, 200)
    refEnd = skipForwMax(text, refEnd, 80)
    refEnd = skipForwMax(text, refEnd, 80)

    # ref section has to start in 2nd half of doc
    if refStart < len(text)/2:
        logging.debug("ignored: refs in 1st half of text")
        return None, None

    return refStart, refEnd

def appendAndCutAll(sections, addStart, addEnd, addName):
    """ 
    given a list of (start, end, name), add (addStart, addEnd, addName) to
    them, but shorten all other elements or remove them so nothing is
    overlapping in the end.
    >>> s = [(0, 915, 'header'), (915, 10526, 'abstract'), (10526, 15783, 'results'), (15783, 16968, 'notes')]
    >>> appendAndCutAll(s, 10000, 14000, "refs")
    [(0, 915, 'header'), (915, 10000, 'abstract'), (10000, 14000, 'refs'), (14000, 15783, 'results'), (15783, 16968, 'notes')]

    """
    logging.log(5, "merging %d-%d into %s" % (addStart, addEnd, sections))
    newSections = []
    for start, end, name in sections:
        trimFts = trimRange(start, end, name, addStart, addEnd)
        newSections.extend(trimFts)

    newSections.append( (addStart, addEnd, addName) )

    logging.log(5, "after merging %d-%d into %s" % (addStart, addEnd, newSections))
    # remove 0-length sections, if any
    newSections = [(start,end,name) for start,end,name in newSections if end-start!=0]
    newSections.sort()
    return newSections

def trimRange(start, end, secName, refStart, refEnd):
    """ 
    trim down range (start,end) so it does not overlap (refStart,refEnd).
    Can potentially split the start-end range into two.
    So returns a list of (start, end, name) features. 
    Can also return an empty list, if start-end is completely covered.

    >>> trimRange(1, 30, "t", 5, 15) # included
    [(1, 5, 't'), (15, 30, 't')]
    >>> trimRange(1, 10, "t", 5, 15) # start overlap
    [(1, 5, 't')]
    >>> trimRange(1, 30, "t", 1, 15) # included
    [(1, 1, 't'), (15, 30, 't')]
    >>> trimRange(1, 30, "t", 7, 13) # included
    [(1, 7, 't'), (13, 30, 't')]
    >>> trimRange(1, 30, "t", 1, 30) # included
    [(1, 1, 't')]
    >>> trimRange(1, 30, "t", 1, 30) # included
    [(1, 1, 't')]
    >>> trimRange(1, 30, "t", 15, 45) # overlap at end
    [(1, 15, 't')]
    >>> trimRange(1, 30, "t", 150, 200) # no overlap
    [(1, 30, 't')]
    """
    sections = []
    # possible overlaps:
    #  <---------1-------------><------->
    #       <------refs------->
    #  <---2---><---3----><-4--><---5--->
    # case 1: section includes refs -> split section
    if start <= refStart and end > refEnd:
        sections.append((start, refStart, secName))
        sections.append((refEnd, end, secName))
    # case 2: section overlaps refStart -> trim section at front
    elif start <= refStart and end > refStart:
        sections.append((start, refStart, secName))
    # case 3: section is included in refs -> skip
    elif start >= refStart and end <= refEnd:
        return sections
    # case 4: section overlaps refEnd -> trim section at end
    elif start < refEnd and end >= refEnd:
        # no need to add the refs section, has been done before
        sections.append((refEnd, end, secName))
    # case 5: no overlap -> just add
    else:
        sections.append((start, end, secName))
    return sections

def sectionSplitter(text, fileType, refMinLen=500, minDist=MINSECLEN):
    """ 
    split file into sections. yields tuples with (start, end, section) 
    Based on keywords that have to appear at the beginning of lines
    Additional filtering to ignore the pseudo-sections in structured abstracts
    Sections closer together than minDist are ignored (must be TOCs).

    The reference section uses a completely different approach based on clusters 
    of family names
    Used like this:
    sections = sectionSplitter(text, fileData.fileType)

    returns list of tuples (start, end, section)
    section is one of:
    'abstract','intro','methods','results','discussion','conclusions','ack','refs', 'notes'
    or 'unknown' or 'supplement'
    """
    logging.debug("section splitting")
    # find sections based on keywords on lines
    if fileType=="supp":
        return [(0, len(text), "supplement")]

    # try name-clustering based method to find ref section
    refStart, refEnd = findRefSection(text)
    keywordBasedRefs = False
    if refStart==None or refEnd-refStart < refMinLen:
        logging.debug("found no ref section using names, or ref section found too short")
        keywordBasedRefs=True

    # now try keyword based method to find other sections
    sections = sectionRangesKeyword(text, minDist, doRefs=keywordBasedRefs)
    if sections==None:
        sections = [(0, len(text), "unknown")]

    # cut ref section out from other sections
    if not keywordBasedRefs:
        sections = appendAndCutAll(sections, refStart, refEnd, "refs")

    logging.log(5, "final sections: %s" % sections)
    return sections

# --- frequently used English words -----

commonWords = None

def initCommonWords(listName="top1000"):
    """ read BNC wordlists into memory
    listName is one of top1000, verbs
    >>> initCommonWords()
    >>> isCommonWord("doing")
    True
    """
    global commonWords
    commonWords = set()
    fname = join(pubConf.staticDataDir, "bnc", listName+".txt")
    for line in open(fname):
        if "_" in line: # multiword expressions
            continue
        word = line.rstrip("\n")
        commonWords.add(word)

def isCommonWord(w):
    " this is somewhat slower than to use the commonWords set directly "
    if commonWords==None:
        initCommonWords()
    return (w in commonWords)

# - cleaning of sentences -

def sectionSentences(text, fileType="", minSectDist=MINSECLEN, minChars=30, \
        minWords=5, maxLines=10, minSpaces=4, mustHaveVerb=True):
    """
    Try split the text into sections and these into clean 
    grammatically parsable English sentences. Skip the reference
    section of the text. Skip too long, too short
    and sentences that go over too many lines or sentences that don't contain a
    common English word.  These are all most likely garbage (e.g. html menus,
    TOCs, figures, tables etc).   
    Yields tuples (section, start, end, sentence). Sentence has newline replaced
    with space.
    >>> text = "           \\nIntroduction\\n                                                         \\nMethods\\n no. yes. palim palim. We did something great and were right.\\nResults\\nOur results are very solid\\nand strong and reliable."
    >>> list(sectionSentences(text, minSectDist=1))
    [['methods', 114, 152, 'We did something great and were right.'], ['results', 160, 212, ' Our results are very solid and strong and reliable.']]
    """
    if mustHaveVerb:
        initCommonWords("verbs")

    for secStart, secEnd, section in sectionSplitter(text, fileType, minDist=minSectDist):
        if section=="refs":
            logging.info("Skipping ref section %d-%d" % (secStart, secEnd))
            continue

        # skip the section title ("Results") line
        secStart = skipForwMax(text, secStart, 60)

        secText = text[secStart:secEnd]
        for sentStart, sentEnd, sentence in sentSplitter(secText):
            if sentEnd-sentStart < minChars:
                logging.debug("Sentence too short: %s" % sentence)
                continue

            sentWords = wordSet(sentence)
            if len(sentWords) < minWords:
                logging.debug("Sentence skipped, too few words: %s" % sentence)
                continue

            if mustHaveVerb is True:
                commSentWords = sentWords.intersection(commonWords)
                if len(commSentWords)==0:
                    logging.debug("Sentence skipped, no verb: %s" % sentence)
                    continue
                
            nlCount = sentence.count("\n")
            if nlCount > maxLines:
                logging.debug("Sentence spread over too many lines: %s" % sentence)
                continue

            spcCount = sentence.count(" ")
            if spcCount < minSpaces:
                logging.debug("Sentence has too few spaces: %s" % sentence)
                continue

            sentence = sentence.replace("\n", " ")
            sentence = unidecode.unidecode(sentence)
            yield [section, secStart+sentStart, secStart+sentEnd, sentence]

if __name__ == "__main__":
   import doctest
   doctest.testmod()
 
