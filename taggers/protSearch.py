import logging, re, sys, gzip
from maxMarkov import MarkovClassifier
from os.path import join, dirname
from collections import defaultdict, OrderedDict

#try:
    #import re2 as re
#except ImportError:
import re

DATADIR = join(dirname(__file__), 'data', 'protSearch')

""" 
two parts:

1) map/reduce-style algorithm that can be used to 
output a list of protein-like common words, that are longer than
MINLEN and occur more often than MINCOUNT

2) also an annotator, that can be used to annotate
protein-like sequences longer than MINLEN amino acids

#>>> t = BackgroundWords()
#>>> t.annotRows("hihihi ACYACHY")
#>>> list(t.finalRows())
#[['ACYACHY', '1']]

"""

MINCOUNT=10 # min count of upcase-protein-like word in text collection to be considered a common word (=blacklisted)
MINLEN=5    # min len of word to be looked at

LOWERMAX=3  # maximum number of lowercase letters in a long string of single-letter amino acid chars
MINDIFFCHARS=4 # min number of diff characters in prot word
MINSEQLEN=8 # min length of total sequence of several prot words 
#PREFIXLEN=6 # compare only the first x letters against common words -> removed too many
MAXSEQOCC=5 # max number of times a sequence can appear in a document without getting eliminated
MAXSEQPERDOC=300 # max number of sequences per document, we skip the whole doc otherwise

CLOSESEQDIST=5 # when appending SINGLE letters to the current seq, tolerate a few non-prot characters

isTcrDocRe  = re.compile(r'.*(T.Cell.Receptor| TCR |CDR|T.Cells).*')

nonLetterRe = re.compile(r'[^A-Za-z0-9,;()]') # non-alphanumeric characters, replaced with spaces
wordRe      = re.compile(r'[A-Za-z0-9]+') # a word, any string of letters or numbers
dnaLetters  = set(['A','C','T','G','U','N'])
#dnaLetters  = set(['A','C','T','G','U','N','S','M','W','Y','R'])
protLetters = set("ABCDEFGHIKLMNPQRSTVWXYZ")
lowercaseLetters = set("abcdefghijklmnopqrstuvwxyz")

#upCaseRe = re.compile("[A-Z]")

iupacCodesLong = {
    "ala":"A", # Alanine
    "asx":"B", # aspartic acid
    "cys":"C", # cystein
    "asp":"D", # aspartic acid
    "glu":"E", # glutamic acid
    "phe":"F", # phenylalanine
    "gly":"G", # glycine
    "his":"H", # histidine
    "ile":"I", # isoleucine
    "lys":"K", # lysine
    "leu":"L", # leucine
    "met":"M", # methionine
    "asn":"N", # asparagine
    "pro":"P", # proline
    "gln":"Q", # glutamine
    "arg":"R", # arginine
    "ser":"S", # serine
    "thr":"T", # threonine
    "val":"V", # valine
    "trp":"W", # tryptophane
    "xaa":"X", # any
    "tyr":"Y", # tyrosine
    "glx":"Z", # glutamine
}

class UpcaseCounter:
    """ 
    map-reduce style algorithm to create a list of most common
    protein-like words 
    """
    def __init__(self):
        self.wordCounts = {}
        self.headers = ["word", "articleIds"]
        self.reduceHeaders = ["word", "count"]

    def map(self, article, file, text, results):
        #words = set(text.replace("\a"," ").split())
        text = cleanText(text)
        words = set(text.split())
        for word in words:
            letters = set(word)
            notProtLetters = letters - protLetters
            notDnaLetters = letters - dnaLetters
            if len(word)>=MINLEN and len(notProtLetters)==0 and len(notDnaLetters)>1:
                results.setdefault(word, set())
                if article!=None:
                    results[word].add(article.articleId)
                else:
                    results[word].add(file.fileId)

    def reduce(self, word, artSets):
        sum = 0
        articleIds = set()
        for artSet in artSets:
            articleIds.update(artSet)
        if len(word)<MINLEN:
            yield None
        if len(articleIds)<MINCOUNT:
            yield None
        else:
            yield [word, len(set(articleIds))]

def detectProteinWordClean(word, blacklist, lastWord, pos, distLastWord, stack):
    """
    Checks if a word is a protein code and return it as a clean AA-seq.

    Recognizes:
        - isolated three letter iupac codes
        - dash-sep. series or three letter iupac codes
        - isolated single amino acids (but only if preceded by other single letter amino acids)
        - series of single letter codes
    """
    word = word.strip()
    letters = set(word)
    lowerCount = len(letters.intersection(lowercaseLetters))

    logging.log(5, "wordLen %d, letterLen %d, lowerCount %d" % (len(word), len(letters), lowerCount))
    # case1: a code like "met" 
    if word.lower() in iupacCodesLong:
        logging.log(5, "%s is iupacCode" % word)
        return iupacCodesLong[word.lower()], True

    # case2: a string like "met-ala-phe"
    if "-" in word:
        parts = word.split("-")
        partLengths = [len(w) for w in parts]
        avgLen = sum(partLengths) / len(parts)
        if avgLen==3:
            logging.log(5, "%s looks like hyphen-sep iupac codes" % word)
            aaList = [iupacCodesLong.get(p.lower(), "") for p in parts]
            aaString = "".join(aaList)
            if len(aaString)>0:
                return aaString, True
            else:
                return None, None

    # case3: a single letter, close enough to the last word AND
    # EITHER within a already started protein seq (=stack is non-empty)
    # OR stack is empty AND preceded by a single AA letter in which case we add both to the stack
    #print word in protLetters, distLastWord, stack, lastWord
    if word in protLetters and distLastWord<CLOSESEQDIST and (len(stack)!=0 or lastWord in protLetters):
        if len(stack)==0:
            stack.append((lastWord, pos-distLastWord, pos))
        return word, False

    # case3: word composed exclusively of uppercased AA-chars
    if len(word)>=MINLEN and \
     len(letters)>=MINDIFFCHARS and \
     lowerCount <= LOWERMAX:
        # enough diff uppercase letters
        logging.log(5, "Interesting word: %s" % word)
        notProtLetters = letters - protLetters
        notProtCount = len(notProtLetters)
        dnaRatio = float(len([l for l in word if l in dnaLetters])) / float(len(word))
        if notProtCount==0:
            return word, False

    return None, None

def cleanText(text):
    # clean: non-letters -> spaces
    cleanText = nonLetterRe.sub(" ", text)
    return cleanText

def countChanges(stackWord):
    """ count how often on average we encounter a change between letters in a word 
    >>> countChanges("AAAAAAAA")
    0.0
    >>> countChanges("ACACACACACAC")
    0.9166666666666666
    >>> countChanges("STDNNNTKTISTDNNNTKTIC")
    0.7619047619047619
    >>> countChanges("IGIGIGGIGIGIGIGIGIGIGIGIGIGIGGGGGIGGGGGGGGGGGGGGIGGGG")
    0.5849056603773585
    """
    lastChar = stackWord[0]
    diffCount = 0.0
    for i in range(1, len(stackWord)):
        char = stackWord[i]
        if lastChar!=char:
            diffCount += 1
        lastChar = char
    return diffCount / len(stackWord)
        

def generateRow(stack, sureProt, classifier, blacklist):
    """ given a stack, a classifier and blacklist, output a tab-sep row for the final output file. 
    set sureProt to True to indicate that we are certain that this is a protein sequence
    
    """

    stackWord = "".join([w for w,x,y in stack])

    if len(stackWord) < MINSEQLEN:
        logging.log(5, "stack is too short: %s" % stackWord)
        return None

    # make sure it's not DNA and not blacklisted
    dnaRatio = float(len([l for l in stackWord if l in dnaLetters])) / float(len(stackWord))
    if not sureProt and dnaRatio>0.85:
        logging.log(5, "whole stack looks like DNA: %s" % stackWord)
        return None

    if not sureProt and stackWord in blacklist:
        logging.log(5, "full stack is blacklisted: %s" % stack)
        return None

    stackPrefix = stackWord[:6]
    logging.debug("prefix is %s" % stackPrefix)
    if not sureProt and stackPrefix in blacklist:
        # logging.log(5, "prefix of stack is blacklisted: %s" % stackPrefix)
        # return None
        prefixAccept = "N"
    else:
        prefixAccept = "Y"

    stackSuffix = stackWord[-6:]
    logging.debug("suffix is %s" % stackSuffix)
    if not sureProt and stackSuffix in blacklist:
        suffixAccept = "N"
    else:
        suffixAccept = "Y"

    # check that whole stack contains enough different letters
    stackLetters = set(stackWord)
    diffLetterPerChar = len(stackLetters) / float(len(stackWord))
    if not (sureProt or (diffLetterPerChar > 0.3 or len(stackWord) > 30)):
        logging.log(5, "stack %s is not a sequence, too short/too few avg diff letters: %f" % \
            (stackWord, diffLetterPerChar))
        return None

    # check that letters change enough
    avgChange = countChanges(stackWord)
    if avgChange<0.4:
        logging.log(5, "stack %s has not enough letter changes: %d" % \
            (stackWord, avgChange))
        return None

    start = stack[0][1]
    end   = stack[-1][-1]
    classType, markovClass = classifier.classify(stackWord)
    if classType=="neg":
        markovAccept = "N"
    else:
        # "unsure" and "pos" are both accepted as proteins
        markovAccept = "Y"

    if markovAccept=="Y" and prefixAccept=="Y" and suffixAccept=="Y":
        verdict = "isPep"
    else:
        verdict = "isEng"

    row = [start, end, stackWord, len(stack), prefixAccept, suffixAccept, markovAccept, verdict, markovClass]
    return row

def findProteins(text, blacklist, classifier, docId=""):
    """
    detect proteins, combining a markov classifier, a blacklist and 4-letter prefixes of the blacklist

    >>> markov = loadMarkovClassifier()
    >>> findProteins("  5 10 15 20 Ala-Val-Thr-Lys-Gly-Thrlle-Asn-Asp-Pro-Gln-Ala-Ala-Lys-Glu-Ala-Leu-Asp-Lys-Tyr. ", [], markov)
    [[40, 91, 'NDPQAAKEALDKY', 13, 'Y', 'Y', 'Y', 'isPep', 'uniprot']]

    # would be nice if the result looked like this: how?
    #[[13, 91, 'AVTKGNDPQAAKEALDKY', 1, 'Y', 'uniprot']]

    >>> findProteins("  N A F T K A T P L S T Q V Q L S M C A D V P L V V E Y A ", [], markov)
    [[2, 57, 'NAFTKATPLSTQVQLSMCADVPLVVEYA', 28, 'Y', 'Y', 'Y', 'isPep', 'uniprot']]
    """

    seqRows = OrderedDict()

    stack = []
    seqCount = 0
    lastWord = ""
    lastPos = 0

    for match in wordRe.finditer(text):
        word = match.group().strip()
        pos = match.start()
        dist = pos - lastPos

        seq, sureProt = detectProteinWordClean(word, blacklist, lastWord, pos, dist, stack)

        lastPos = pos
        lastWord = word

        # if we have a sequence, just append to the stack
        if seq!=None:
            logging.log(5, "Adding %s to stack" % seq)
            stack.append([seq, match.start(), match.end()])
            continue

        # OK, we don't have a sequence-like word 
        # just go to next word if we're currently not in a protein sequence
        if len(stack)==0:
            continue

        # OK, we don't have a sequence like word BUT we had some protein sequence on the left
        # If the last seq is really close, just skip this word, might be noise
        lastSeqDist = pos-stack[-1][1]
        if lastSeqDist < 3:
            logging.log(5, "skipping word %s, not processing stack" % word)
            continue

        # If we're far away from the last sequence and we have a full stack, output and reset the stack
        if len(stack)>0:
            row = generateRow(stack, sureProt, classifier, blacklist)
            stack = []
            if row!=None:
                seq = row[2]
                seqRows.setdefault(seq, []).append(row)
                seqCount += 1

            # in case that we run into a genbank/uniprot supplemental table, skip the whole doc
            if seqCount > MAXSEQPERDOC:
                logging.warn("%d proteins, too many, in document %s, skipping whole document" % (seqCount, docId))
                logging.warn("%s" % seqRows.keys())
                return []

    # process last stack
    if len(stack)>0:
        row = generateRow(stack, sureProt, classifier, blacklist)
        if row!=None:
            seq = row[2]
            seqRows.setdefault(seq, []).append(row)

    # post-processing: remove too common seqs
    cleanRows = []
    for seq, rows in seqRows.iteritems():
        if len(rows)>MAXSEQOCC:
            logging.debug("Seq %s seen more than %d times, skipping this whole sequence" % (seq, len(rows)))
        else:
            cleanRows.extend(rows)

    return cleanRows

def loadMarkovClassifier():
    # init markov classifier from files
    markov = MarkovClassifier(0.02)
    #fgFnames = [join(DATADIR, fname) for fname in ["cdr3.markov", "uniprot.markov"]]
    fgFnames = [join(DATADIR, fname) for fname in ["uniprot.markov"]]
    bgFnames = [join(DATADIR, "medline.markov")]
    markov.loadModels(2, fgFnames, bgFnames)
    return markov

class Annotate:
    """ annotator to find protein sequence in english text """
    def __init__(self):
        self.headers = ["start", "end", "seq", "partCount", "prefixFilterAccept", \
            "suffixFilterAccept", "markovFilterAccept", "verdict", \
            "markovName", "isTcrDoc", "snippet"]
        self.requireParameters=1
        self.excludeWords = set()

        self.markov = loadMarkovClassifier()
        self.seenSeqs = set()

    def startup(self, parameters):
        #if "wordFile" not in parameters:
            #logging.error("algorithm requires one parameter called 'wordFile':")
            #logging.error("a filename with a list of common words that look like proteins")
            #sys.exit(1)

        #excludeFname = parameters.get("wordFile", None)
        #assert(excludeFname!=None) # need to specify "wordFile" parameter to algorithm
        excludeFname = join(DATADIR, "commonFilteredWords.txt.gz")
        logging.info("Reading %s" % excludeFname)
        for line in gzip.open(excludeFname):
            word = line.strip().split()[0]
            self.excludeWords.add(word)

        prefixList = [s[:6] for s in self.excludeWords if len(s)>5]
        suffixList = [s[-6:] for s in self.excludeWords if len(s)>5]
        self.excludeWords.update( prefixList)

        nameFname = join(DATADIR, "commonNames.txt.gz")
        logging.info("Reading %s" % nameFname)
        for name in gzip.open(nameFname):
            name = name.rstrip("\n")
            self.excludeWords.add(name)
            if len(name)>5:
                self.excludeWords.add(name[:6])
                self.excludeWords.add(name[-6:])

    def annotateFile(self, articleData, fileData):
        text = cleanText(fileData.content)
        isTcrDoc = (isTcrDocRe.match(text, re.IGNORECASE)!=None)
        rows = findProteins(text, self.excludeWords, self.markov, docId=articleData.externalId+"/"+fileData.desc)
        for row in rows:
            row.append(isTcrDoc)
            yield row

def test():
   #test1="(0)VGGVMHCFTGSYETMKKAVDMG-----FFISYSGILTYKNAESVREVAKRTPTSRILLETDSPFLA"
   markov = loadMarkovClassifier()
   rows = findProteins("  5 10 15 20 Ala-Val-Thr-Lys-Gly-Thrlle-Asn-Asp-Pro-Gln-Ala-Ala-Lys-Glu-Ala-Leu-Asp-Lys-Tyr. ", [], markov)
   [[13, 91, 'AVTKGNDPQAAKEALDKY', 18, 'Y', 'uniprot']]

   rows = findProteins("  N A F T K A T P L S T Q V Q L S M C A D V P L V V E Y A ", [], markov)
   assert(rows==[[2, 57, 'NAFTKATPLSTQVQLSMCADVPLVVEYA', 28, 'Y', 'Y', 'Y', 'isPep', 'uniprot']])

   #rootLog = logging.getLogger('')
   #rootLog.setLevel(5)
   #logging.basicConfig(level=5,
                       #format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                       #datefmt='%m-%d %H:%M', stream=sys.stdout)
   rows = findProteins("  N - A - F - T-K-A-T-P-L-", [], markov)
   assert(rows == [[2, 25, 'NAFTKATPL', 9, 'Y', 'Y', 'Y', 'isPep', 'uniprot']])

   rows = findProteins("   K K V L E A L K D L I N E A C W D I S S S G V N L Q S M   ", [], markov)
   assert(rows == [[3, 58, 'KKVLEALKDLINEACWDISSSGVNLQSM', 28, 'Y', 'Y', 'Y', 'isPep', 'uniprot']])

   rows = findProteins("   EEEEEEEEEEEEEEEEEEEEEEEEEEEEESSGYBA   ", [], markov)
   assert(rows == [])

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    test()
