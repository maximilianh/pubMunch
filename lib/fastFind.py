# fast search for combination of words using dictionaries

import doctest, types, gzip, marshal, optparse, sys, codecs, logging
from os.path import *

# as we use the regexes only to find \w, the original re module 
# is 3 times faster than re2
# It is also 2 times faster than the regex module
import re

#WORDRE = re.compile(r"[\w'[\]()-]+")
# a word is defined to include apostrophes, but not parentheses and dashes, by default
WORDRE = re.compile(r"[\w']+")

# symbols include bands and therefore can include dots
SYMRE = re.compile(r"[\w'.]+")

DASHWORDRE = re.compile(r"[\w'-]+")

def recursiveAdd(dict, wordList, id):
    """
    recursively add the words from wordList to dictionary 
    """
    if len(wordList)==0:
        if None in dict:
            raise Exception("duplicate phrase for id: %s" % id)
        dict[0]=id # 0 terminates phrase
        return dict
    else:
        subDict = dict.get(wordList[0], {})
        recursiveAdd(subDict, wordList[1:], id)
        dict[wordList[0]]=subDict
        return dict

def onlyWords(words):
    " given a list of tuples, only return the last part of the tuples "
    return [w[-1] for w in words]

def leftFlank(l, pos, dist):
    """ get dist elements to the left of pos in list l 
    (pos itself is not included)
    >>> leftFlank("12345", 2, 2)
    '12'
    >>> leftFlank("12345", 0, 2)
    ''
    """
    minPos = max(pos-dist, 0)
    return l[minPos:pos]

def rightFlank(l, pos, dist):
    """ get dist elements to the right of pos in list l, pos itself is not included
    (for zero-based, half-open coordinates)
    >>> rightFlank("12345", 2, 2)
    '45'
    >>> rightFlank("12345", 4, 2)
    ''
    """
    fromPos = min(pos+1, len(l))
    maxPos = min(pos+dist+1, len(l))
    return l[fromPos:maxPos]
    
def constructLex(keywordList, wordRe=WORDRE):
    """
    converts a list of textstrings and identifiers to
    nested dictionaries that allow faster matching
    >>> constructLex( [("q1", ["how are you"]), ("q2", ["do you"])] )
    {'how': {'are': {'you': {0: 'q1'}}}, 'do': {'you': {0: 'q2'}}}
    """
    wordDict = {}
    for id, stringList in keywordList:
        #print id, stringList
        for wordString in stringList:
            #words = wordString.split(" ")
            words = wordRe.findall(wordString)
            recursiveAdd(wordDict, words, id)
    return wordDict

def recursiveFind(words, wordIdx, searchDict, results, flankCount=0, hitStart=None, hitStartIdx=None):
    """
    looks up all words in searchDict if they match words[wordIdx].
    Append results to results list as (start, end, identifier).
    Look for longest non-overlapping matches.

    recursive calls set:
    - hitStart to keep track of the start of the first initial match. 
    - hitStartIdx to keep track of the word index of the first match
    """
    if wordIdx >= len(words):
        return results

    start, end, word = words[wordIdx]
    if hitStart is not None:
        start = hitStart
    else:
        hitStartIdx = wordIdx
    # check current word
    assert(searchDict!=None)
    matchDict = searchDict.get(word, -1)
    if matchDict==-1: # -1 = not found
        return results
    elif type(matchDict)==types.DictType:
        oldResCount = len(results)
        if oldResCount>0:
            lastMatch = results[-1]
        recursiveFind(words, wordIdx+1, matchDict, results, flankCount, start, hitStartIdx)
        # accept, if no other (=longer) match found and 
        # word is last of search string (matchDict contains 0)
        # and lastMatch is not overlapping with this match
        if oldResCount==len(results) and \
          0 in matchDict and \
          (oldResCount==0 or lastMatch[1] < start):
            if flankCount!=0:
                leftWords = onlyWords(leftFlank(words, hitStartIdx, flankCount))
                rightWords = onlyWords(rightFlank(words, wordIdx, flankCount))
                hit = (start, end, str(matchDict[0]), leftWords, rightWords)
            else:
                hit = (start, end, str(matchDict[0]))
            results.append( hit )

def splitText(text, wordRe=WORDRE):
    """
    given a string, gets the start and end
    positions of all words and returns a list
    >>> splitText("brown fox")
    [(0, 5, 'brown'), (6, 9, 'fox')]
    """
    words = [(m.start(), m.end(), m.group()) for m in wordRe.finditer(text)]
    return words

def fastFind(text, lex, wordRe=WORDRE, toLower=False):
    """ find matches of keyword strings in text by splitting text first and then matching
        with dictionaries. For overlaps, returns only longest match.
    >>> lex = constructLex([("p1", ["how are"]), ("p2", ["you doing", "are you"]), ("p3", ["how are you"])])
    >>> test = "how   are  you doing?"
    >>> fastFind (test, lex)
    [(0, 14, 'p3')]
    >>> lex = constructLex([("p1", ["guinea pigs","Pichia pastoris"]), ("p2", ["pig"])])
    >>> test = "I  hate    guinea pigs. I do"
    >>> fastFind (test, lex)
    [(11, 22, 'p1')]
    >>> fastFind ("Pichia pastoris .", lex)
    [(0, 15, 'p1')]
    >>> fastFind ("pichia pastoris .", lex)
    []
    >>> lex = constructLex([("p1", ["alzheimer's disease"]), ("p2", ["pig"])])

    # apostrophes are part of the default word
    >>> fastFind ("alzheimer's disease", lex)
    [(0, 19, 'p1')]

    # flanking brackets can be a problem if they are defined to be part of a word
    >>> fastFind ("(alzheimer's disease)", lex, wordRe=re.compile(r"[\w'-()]+"))
    []

    # flanking brackets are no problem
    >>> fastFind ("(alzheimer's disease)", lex)
    [(1, 20, 'p1')]

    We ignore non-word characters
    >>> fastFind ("(alzheimer's **disease)", lex)
    [(1, 22, 'p1')]

    >>> fastFind ("(AlzHEImer's **Disease)", lex, wordRe=re.compile(r"[\w'-]+"), toLower=True)
    [(1, 22, 'p1')]
    >>> fastFind ("(AlzHEImer's-**Disease)", lex, wordRe=re.compile(r"[\w']+"), toLower=True)
    [(1, 22, 'p1')]
    """
    assert(lex!=None)
    if toLower:
        text = text.lower()
    words = splitText(text, wordRe)
    matches = []
    for i in range(0, len(words)):
        recursiveFind(words, i, lex, matches)
    return matches


def fastFindFlankWords(text, lex, wordDist=1, wordRe=WORDRE):
    """
    do normal matching, but also add the flanking words to left and right to the annotation tuples
    >>> lex = constructLex([("p1", ["hi there"]), ("p2", ["bababa"])])
    >>> lex
    {'hi': {'there': {0: 'p1'}}, 'bababa': {0: 'p2'}}
    >>> fastFindFlankWords ("word0 word1 word2 hi there word5 word6 ", lex, wordDist=2)
    [(18, 26, 'p1', ['word1', 'word2'], ['word5', 'word6'])]
    """
    assert(lex!=None)
    words = splitText(text, wordRe)
    matches = []
    for i in range(0, len(words)):
        recursiveFind(words, i, lex, matches, flankCount=wordDist)
    return matches

    #annotMatches = []
    #for m in matches:
        #start, end, hitId = m
        #left = leftFlank(text, start, 40)
        #leftWords = tuple(wordRe.findall(left)[-wordDist:])

        #right = rightFlank(text, end, 40)
        #rightWords = tuple(wordRe.findall(right)[:wordDist])
        #newRow = (start, end, hitId, leftWords, rightWords)
        #annotMatches.append( newRow )

    #return annotMatches

def _lexIter(fileObj, toLower=False):
    """ parse a tab-sep file (identifier<tab>name1|name2|name3|...) 
    or alternatively a file with just one identifier per line 
    and yield as a list of tuples [(name1, identifier), ...]
    """
    strings = []
    for line in fileObj:
        if line.startswith("#"):
            continue
        fields = line.strip("\n").split("\t")
        if len(fields)==1:
            id, nameString = fields[0], fields[0]
        else:
            id, nameString = fields
        if toLower:
            nameString = nameString.lower()
        names = nameString.split("|")
        #for name in names:
        yield (id, names)

def loadLex(fname):
    " load compiled dictionary (=gziped marshalled file) "
    data = gzip.open(fname).read()
    lex = marshal.loads(data)
    return lex

def parseDict(fname, wordRe=WORDRE, toLower=False):
    """
    reads file (identifier<tab>name1|name2|name3|...) 
    and return as nested dictionaries for fastFind()
    """ 
    if fname.endswith(".gz"):
        fileObj = gzip.open(fname)
    else:
        fileObj = open(fname)
    return constructLex(_lexIter(fileObj, toLower), wordRe=wordRe)

def test():
    """
    >> test()
    ["'how    are'", 'keyword1', 3, 13]
    ["'are you'", 'keyword2', 10, 17]
    ["'how do'", 'keyword3', 19, 25]
    ["'how are'", 'keyword1', 34, 41]
    ["'are you'", 'keyword2', 38, 45]
    """
    #searchStrings = readTabSep(open("dict.tab"))
    #text = "hi how    are you? how do you do? how are you   doing?"
    #matches = fastFind(text, searchStrings)
    #for match in matches:
        #start, end, id = match
        #print [repr(text[start:end]), id, start, end]

def _test():
    import doctest
    doctest.testmod()

def writeLex(lex, fname):
    "  write dictionary as marshalled data to fname "
    #lex = constructLex(idTermList)
    str = marshal.dumps(lex)
    binFile = gzip.open(fname, "wb")
    binFile.write(str)
    logging.info("Wrote compiled dictionary to %s" % fname)

def compileDict(dictFname, wordRe=WORDRE, toLower=False):
    """
    convert dictionary file to memory data structure and write to gzipped marshalled file 
    dictionary file lines have format <identifier> tab <name1>|<name2>|... 
    """
    lex = parseDict(dictFname, wordRe, toLower)
    dictBase = basename(dictFname).split(".")[0]
    fname = join(dirname(dictFname), dictBase+".marshal.gz")
    writeLex(lex, fname)

if __name__ == "__main__":
    parser = optparse.OptionParser("usage: %prog [options] dictFile files - scan files for strings") 
    #parser.add_option("d", "--dictDir", dest="test", action="store_true", help="do something") 
    parser.add_option("-t", "--test", dest="test", action="store_true", help="run tests") 
    (options, args) = parser.parse_args()
    if options.test:
        _test()
    elif len(args)==0:
        parser.print_help()
    else:
        dictFname = args[0]
        sys.stderr.write("Reading dict\n")
        lex = loadLex(dictFname)
        sys.stderr.write("Annotating files\n")
        for fname in args[1:]:
            text = codecs.open(fname, "r", "utf8").read()
            for start, end, id in fastFind(text, lex):
                fname = splitext(basename(fname))[0]
                id = "species:ncbi:"+id
                data = [id, fname, str(start),str(end)]
                data.append(text[start:end])
                line = "\t".join(data)
                print line.encode("utf8")


