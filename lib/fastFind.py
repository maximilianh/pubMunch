# fast search for combination of words using dictionaries

import doctest, types, re, gzip, marshal, optparse, sys, codecs
from os.path import *

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

def constructLex(keywordList):
    """
    converts a list of textstrings and identifiers to
    nested dictionaries that allow faster matching
    >>> constructLex( [("q1", ["how are you"]), ("q2", ["do you"])] )
    {'how': {'are': {'you': {0: 'q1'}}}, 'do': {'you': {0: 'q2'}}}
    """
    wordDict = {}
    for id, stringList in keywordList:
        for wordString in stringList:
            words = wordString.split(" ")
            recursiveAdd(wordDict, words, id)
    return wordDict

def recursiveFind(wordList, wordIdx, searchDict, results, firstStart=None):
    """
    looks up all words in searchDict if they match wordList[wordIdx]

    recursive calls set:
    - firstStart to keep track of the start of the first initial match. 
    """
    if wordIdx >= len(wordList):
        return results

    start, end, word = wordList[wordIdx]
    if firstStart!=None:
        start = firstStart
    # check current word
    assert(searchDict!=None)
    matchDict = searchDict.get(word, -1)
    if matchDict==-1: # -1 = not found
        return results
    elif type(matchDict)==types.DictType:
        oldResCount = len(results)
        if oldResCount>0:
            lastMatch = results[-1]
        recursiveFind(wordList, wordIdx+1, matchDict, results, firstStart=start)
        # accept, if no other (=longer) match found and 
        # word is last of search string (matchDict contains 0)
        # and lastMatch is not overlapping with this match
        if oldResCount==len(results) and \
          0 in matchDict and \
          (oldResCount==0 or lastMatch[1] < start):
            results.append( (start, end, str(matchDict[0]) ))

def splitText(text, wordRegex):
    """
    given a string, gets the start and end
    positions of all words and returns a list
    >>> splitText("brown fox", "\w+")
    [(0, 5, 'brown'), (6, 9, 'fox')]
    """
    words = []
    splitRe = re.compile(wordRegex)
    for match in splitRe.finditer(text):
        wordInfo = (match.start(), match.end(), match.group())
        words.append(wordInfo)
    return words


def fastFind(text, lex, wordRegex="[\\w'[\]()-]+"):
    """ find matches of keyword strings in text by splitting text first and then matching
        with dictionaries. For overlaps, returns only longest match.
        Case matters!
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
    >>> lex = constructLex([("p1", ["alzheimer's disease"]), ("p2", ["pig"])])
    >>> fastFind ("alzheimer's disease", lex)
    [(0, 19, 'p1')]
    """
    words = splitText(text, wordRegex)

    matches = []
    for i in range(0, len(words)):
        recursiveFind(words, i, lex, matches)
    return matches

def _lexIter(fileObj):
    """ parse a tab-sep file (identifier<tab>name1|name2|name3|...) 
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
        names = nameString.split("|")
        for name in names:
            yield (name, id)

def parseLex(fileObj):
    """
    reads file (identifier<tab>name1|name2|name3|...) 
    and return as nested dictionaries for fastFind()

    can also read *.marshal.gz files (faster to load)
    """ 
    if isinstance(fileObj, str):
        if fileObj.endswith(".marshal.gz"):
            data = gzip.open(fileObj).read()
            lex = marshal.loads(data)
            return lex
        elif fileObj.endswith(".gz"):
            fileObj = gzip.open(fileObj)
        else:
            fileObj = open(fileObj)
    return constructLex(_lexIter(fileObj))

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
        lex = parseLex(dictFname)
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


