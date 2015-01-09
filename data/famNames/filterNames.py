from collections import defaultdict
import re, sys
from os.path import join

sys.path.append("../../tools/lib")
import pubConf

wordRe = re.compile(r'\w+', re.U)

MINCOUNT=30
MINLEN=30

blackList = set(["VAN", "DE", "VON", "LE", "LES", "TO", "GRANT", "BLACK", "GREY", "GRAY", "O", "LOGIN"])

# parse BNC
#bncFname = join(pubConf.staticDataDir, "bnc.txt")
bncFname = "/hive/data/outside/pubs/wordFrequency/bnc/bnc.txt"
bncWords = set()
for line in open(bncFname):
    word = line.strip().upper()
    bncWords.add(word)
bncWords.update(blackList)

names = set()

counter = defaultdict(int)
for line in open("authors.tab"):
    name, count, nameLen = line.rstrip("\n").split("\t")
    name = name.decode("utf8")
    count = int(count)

    if count<MINCOUNT:
        continue
    if len(name)>MINLEN:
        continue
    words = wordRe.findall(name)
    if len(words)>5:
        continue
    for w in words:
        w = w.upper()
        # name has to be either very common and can be any word
        # or not so common but not a common English word and not too short
        if (count>15000) or (len(w)>4 and w not in bncWords):
            #print w.encode("utf8")
            if w in blackList:
                continue
            names.add(w)
    #print name.encode("utf8")
    #print repr(words)

ofh = open("commonNames.txt", "w")
for n in names:
    ofh.write( n.encode("utf8"))
    ofh.write("\n")
ofh.close()
