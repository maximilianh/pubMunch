# markov model for sequences and a classifier that is using them
import gc, math, pickle, operator, logging, itertools, re
from os.path import basename, splitext
from collections import defaultdict
from maxbio import openFile

class Markov():
    """ a little markov chain scorer for protein sequences 
    >>> m = Markov(alpha="ACTG")
    >>> m.train(["ACACACCCACACACACCACACTGTG"])
    >>> m.score("TTTTT")
    0.1632879780810205
    >>> m.score("ACACAC")
    1.5307253382591655
    >>> m = Markov(alpha="AB", markovLen=3)
    >>> m.train(["ABAABAABAABAABA"])
    >>> m.printProbs()
    ABA 0.325422400435
    AAB 0.268263986595
    BAA 0.268263986595
    ABB 0.0741079721537
    AAA 0.0741079721537
    BBB 0.0741079721537
    BBA 0.0741079721537
    BAB 0.0741079721537
    >>> m.score("ABA")
    0.325422400434628
    >>> m.score("GATTA") 
    Traceback (most recent call last):
    AssertionError
    """
    def __init__(self, inFname = None, markovLen=2, alpha="ACDEFGHIKLMNPQRSTVWYBXZJUO", checkSeq=True):
        self.alpha = set(alpha)
        self.counts = defaultdict(int)
        self.logProbs = defaultdict(float)
        self.seqLen = 0
        self.markovLen = markovLen
        self.checkSeq = checkSeq
        if inFname!=None:
            self.load(inFname)

    def addSeqCounts(self, seq):
        " add counts of dimers to dict "
        self.seqLen += len(seq)
        seq = seq.upper()
        for i in range(0, len(seq)-(self.markovLen-1)):
            subseq = seq[i:i+self.markovLen]
            self.counts[subseq]+=1

    def calcProbs(self):
        " when finished with addSeqCounts, call this to calc the probabilities "
        self.logProbs = defaultdict(float)
        #allSum = sum(self.counts.values())
        for seqTuple in itertools.product(self.alpha, repeat=self.markovLen):
             seq = "".join(seqTuple)
             count = float(self.counts.get(seq, 1.0))
             prob = count / (self.seqLen-(self.markovLen-1))
             self.logProbs[seq] = math.log1p(prob)
        del self.counts

    def printProbs(self):
        seqProbs = list(self.logProbs.items())
        seqProbs.sort(key=operator.itemgetter(1), reverse=True)
        for seq, prob in seqProbs:
            print(seq, prob)

    def train(self, seqs):
        logging.info("Training of %d seqs..." % len(seqs))
        for seq in seqs:
            if self.checkSeq:
                letters = set(seq)
                assert(len(letters-self.alpha)==0) # no non-alphabet letters in sequence?
            self.addSeqCounts(seq)
        self.calcProbs()

    def score(self, seq):
        " once trained, call this to score a sequence against the model "
        prob = 0.0
        for i in range(0, len(seq)-(self.markovLen-1)):
            subseq = seq[i:i+(self.markovLen)]
            if self.checkSeq:
                letters = set(subseq)
                assert(len(letters-self.alpha)==0) # no non-alphabet letters in sequence?
            logProb = self.logProbs[subseq]
            prob += logProb
        return prob

    def save(self, fname):
        fileObj = openFile(fname, "w")
        logging.info("writing dimers to file...")
        gc.disable()
        pickle.dump(self.logProbs, fileObj)
        gc.enable()

    def load(self, fname):
        self.name = splitext(basename(fname))[0]
        fileObj = openFile(fname)
        logging.info("reading dimers from file...")
        gc.disable()
        self.logProbs = pickle.load(fileObj)
        gc.enable()

class MarkovClassifier(object):
    """ a markov classifier, given a set of foreground and background model filenames, classifies as pos or neg
    if score difference between top foreground and top background is higher than x

    # test without the special dna class
    >>> fg = Markov(alpha="ACTG")
    >>> fg.train(["ACACACCCACACACACCACACTGTG"])
    >>> fg.name = "moreAC"
    >>> bg = Markov(alpha="ACTG")
    >>> bg.train(["CGCGCGCGCGCGCGCGCGCGCG"])
    >>> bg.name = "moreCG"
    >>> mc = MarkovClassifier(0.9, fgModels=[fg], bgModels=[bg], maxDnaShare=None)
    >>> mc.classify("ACTGACTGACTGACT")
    ('pos', 'moreAC')
    >>> mc.classify("CGCGCGCGGCGCC")
    ('neg', 'moreCG')
    >>> mc.classify("ACTGACTGCCGATATACTGACT")
    ('unsure', 'moreAC')
    >>> mc.fgScores, mc.bgScores, mc.diff
    ([('moreAC', 2.162412011914447)], [('moreCG', 1.6945585289009908)], 0.4678534830134562)

    # testing DNA class
    >>> fg = Markov(alpha="DE")
    >>> fg.train(["DEDEDEDE"])
    >>> fg.name = "DE"
    >>> bg = Markov(alpha="DE")
    >>> bg.train(["EEEEEEEE"])
    >>> bg.name = "EE"
    >>> mc = MarkovClassifier(0.9, fgModels=[fg], bgModels=[bg], maxDnaShare=0.8)
    >>> mc.classify("CGCGCGCGGCGCC")
    ('neg', 'dna')
    """
    def __init__(self, minDiff, fgModels=[], bgModels=[], maxDnaShare=0.85, longProtMinLen=40, longProtMinChars=8):
        self.maxDnaShare = maxDnaShare
        self.longProtMinLen = longProtMinLen # min length of seq to call it "longProt"
        self.longProtMinChars = longProtMinChars # min number of different AAs to call it "longProt"

        self.fgModels = fgModels
        self.bgModels = bgModels
        self.minDiff = minDiff
        self.nuclRegex = re.compile("[ACTGU]")

        self.fgScores = None
        self.bgScores = None
        self.diff = None

    def loadModels(self, markovLen, fgModelFnames, bgModelFnames):
        self.fgModels = self._openModels(markovLen, fgModelFnames)
        self.bgModels = self._openModels(markovLen, bgModelFnames)
        assert(len(self.fgModels)!=0 and len(self.bgModels)!=0) # need at least one fg and one bg model

    def _openModels(self, markovLen, modelFnames):
        " given filenames, return modelobjects "
        models = []
        for modelFname in modelFnames:
            m = Markov(modelFname, markovLen=markovLen)
            assert(len(list(m.logProbs.keys())[0])==markovLen) # set -l to correct length when you load a file
            models.append(m)
        return models

    def _scoreSeq(self, models, seq):
        """ given a seq and models, returns a dict with classType -> score """
        assert(len(self.fgModels)!=0 and len(self.bgModels)!=0) # need at least one fg and one bg model
        diff = 0.0
        scoreDict = {}
        for m in models:
            score = m.score(seq)
            scoreDict[m.name]=score
        return scoreDict
        
    def classify(self, seq):
        """ return a tuple ("pos" or "neg" or "unsure", className) given a sequence 
        The classes dna and longProt are shortcuts for speed and take precedence

        also sets the variables fgScores and bgScores and diff for debugging.
        """
        self.fgScores = None
        self.bgScores = None
        self.diff = 0.0

        # dna is it's own class, no need for a markov model 
        if self.maxDnaShare!=None and float(len(self.nuclRegex.findall(seq))) / len(seq) > self.maxDnaShare:
            return ("neg", "dna")

        # quick hack to make it fast on long sequences, unlikely to be not protein if long
        # this is just to speed it up on long sequences
        diffChars = len(set(seq))
        logging.debug("len %d, diffChars %d" % (len(seq), diffChars))
        if len(seq) > self.longProtMinLen and diffChars > self.longProtMinChars:
            return ("pos", "longProt")

        # otherwise run markov models
        fgScores = self._scoreSeq(self.fgModels, seq)
        fgScores = list(fgScores.items())
        fgScores.sort(key=operator.itemgetter(-1), reverse=True)
        self.fgScores = fgScores

        bgScores = self._scoreSeq(self.bgModels, seq)
        bgScores = list(bgScores.items())
        bgScores.sort(key=operator.itemgetter(-1), reverse=True)
        self.bgScores = bgScores

        topFgClass, topFgScore  = fgScores[0]
        topBgClass, topBgScore  = bgScores[0]

        diff = topFgScore - topBgScore
        self.diff = diff

        logging.debug("diff %f, minDiff %f" % (diff, self. minDiff))

        if abs(diff) < self.minDiff:
            logging.debug("small difference, unsure")
            return ('unsure', topFgClass)

        if diff < 0:
            logging.debug("negative score, so negative class")
            return ("neg", topBgClass)
        else:
            logging.debug("positive score, class %s" % topFgClass)
            return ('pos', topFgClass)

if __name__ == "__main__":
    import doctest
    doctest.testmod()


