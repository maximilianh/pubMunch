import logging, re, sys
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

MINCOUNT=10 # min count of word in database to be considered NOT protein
LOWERMAX=4  # maximum number of lowercase letters
MINLEN=5    # min len of word to be looked at
MINDIFFCHARS=4 # min number of diff characters in prot word
MINSEQLEN=8 # min length of total sequence of several prot words 
#PREFIXLEN=6 # compare only the first x letters against common words -> removed too many

nonLetterRe = re.compile(r'[^A-Za-z]') # non-alphanumeric characters, replaced with spaces
wordRe      = re.compile(r'[A-Za-z-]+') # a word can include dashes
dnaLetters  = set(['A','C','T','G','U','N','S','M','W','Y','R'])
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

    def reduce(self, word, articleIds):
        sum = 0
        articleIds = set(articleIds)
        if len(word)<MINLEN:
            yield None
        if len(articleIds)<MINCOUNT:
            yield None
        else:
            yield [word, len(set(articleIds))]

def looksLikeProtein(word, excludeWords):
    # only AA but not only DNA 
    # and not a common word
    word = word.strip()
    letters = set(word)
    lowerCount = len(letters.intersection(lowercaseLetters))

    logging.log(5, "wordLen %d, letterLen %d, lowerCount %d" % (len(word), len(letters), lowerCount))
    # case1: "met"
    if word.lower() in iupacCodesLong:
        logging.log(5, "%s is iupacCode" % word)
        return iupacCodesLong[word.lower()]

    # case2: "met-ala-phe"
    parts = word.split("-")
    partLengths = [len(w) for w in parts]
    avgLen = sum(partLengths) / len(partLengths)
    if avgLen==3:
        logging.log(5, "%s looks like hyphen-sep iupac codes" % word)
        aaList = [iupacCodesLong.get(p.lower(), "") for p in parts]
        aaString = "".join(aaList)
        if len(aaString)>0:
            return aaString
        else:
            return None

    # case3: CCMTHHPMPPU...
    if len(word)>=MINLEN and \
     len(letters)>=MINDIFFCHARS and \
     lowerCount <= LOWERMAX:
        # enough diff uppercase letters
        logging.log(5, "Interesting word: %s" % word)

        notProtLetters = letters - protLetters
        notProtCount = len(notProtLetters)
        dnaRatio = float(len([l for l in word if l in dnaLetters])) / float(len(word))
        commonWord = (word in excludeWords)
        #commonWord = (word[:PREFIXLEN] in excludeWords)

        logging.log(5, "nonProt: %s dnaRatio: %f commonWord: %s" % \
            (notProtLetters, dnaRatio, str(commonWord)))

        if  notProtCount==0 and dnaRatio<0.9 and not commonWord:
            return word
    return None

def cleanText(text):
    # clean: non-letters -> spaces
    cleanText = nonLetterRe.sub(" ", text)
    return cleanText

class Annotate:
    """ annotator to find protein sequence in english text """
    def __init__(self):
        self.headers = ["start", "end", "seq", "partCount", "snippet"]
        self.requireParameters=1
        self.excludeWords = set()

    def startup(self, parameters):
        if "wordFile" not in parameters:
            logging.error("algorithm requires one parameter called 'wordFile':")
            logging.error("a filename with a list of common words that look like proteins")
            sys.exit(1)

        excludeFname = parameters.get("wordFile", None)
        assert(excludeFname!=None) # need to specify "wordFile" parameter to algorithm
        for line in open(excludeFname):
            word = line.strip().split()[0]
            self.excludeWords.add(word)
            #self.excludeWords.add(word[:PREFIXLEN])

    def annotateFile(self, articleData, fileData):
        text = cleanText(fileData.content)
        stack = []
        rows = []

        for match in wordRe.finditer(text):
            word = match.group().strip()

            word = looksLikeProtein(word, self.excludeWords)
            if word!=None:
                    logging.log(5, "Adding %s to stack" % word)
                    stack.append([word, match.start(), match.end()])
            elif len(stack)>0:
                stackWord = "".join([w for w,x,y in stack])
                stackLetters = set(stackWord)
                diffLetterPerChar = len(stackLetters) / float(len(stackWord))
                if len(stackWord)>MINSEQLEN and  \
                    (diffLetterPerChar > 0.3 or len(stackWord)>30):
                    # check that whole stack contains enough different letters
                    start = stack[0][1]
                    end   = stack[-1][-1]
                    #yield (start, end, stackWord, len(stack))
                    row = [start, end, stackWord, len(stack)]
                    rows.append(row)
                else:
                    logging.log(5, "Skipping stack %s, too short/too few avg diff letters: %f" % \
                        (stackWord, diffLetterPerChar))
                stack = []

        if len(rows)>1000:
            logging.warn("Too many proteins in document, skipping")
            return []
        else:
            return rows

def test():
   rootLog = logging.getLogger('')
   rootLog.setLevel(5)
   logging.basicConfig(level=5,
                       format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                       datefmt='%m-%d %H:%M', stream=sys.stdout)
   #test1="(0)VGGVMHCFTGSYETMKKAVDMG-----FFISYSGILTYKNAESVREVAKRTPTSRILLETDSPFLA"
   test1="(0)VGGVMHCFTGSYETMKKAVDMG"
   print test1
   test1=cleanText(test1)
   print "__",test1,"__"
   assert(looksLikeProtein(test1, [])!=None)
   test2 = "ALA-MET-PHY-ALA-GLU-ASP"
   assert(looksLikeProtein(test2, [])!=None)
   print("iupacTest", looksLikeProtein(test2, []))

if __name__ == "__main__":
    test()
