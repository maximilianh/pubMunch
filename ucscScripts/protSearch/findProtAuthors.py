from maxCommon import *
from collections import defaultdict

# find author names that look like protein sequences and appear more than twice in medline

# prep input data:
# zcat /hive/data/inside/pubs/text/medline/*.articles.gz | cut -f17 > authors.txt

protLetters = set("ABCDEFGHIKLMNPQRSTVWXYZ")

names = defaultdict(int)

for row in iterTsvRows("authors.txt"):
    authors = row.authors.split(";")
    for au in authors:
        # Derewenda, Urszula
        parts = au.strip().split(", ")

        if len(parts)==0:
            continue
        longFamName = parts[0]
        for famName in longFamName.split("-"):
            famName = famName.strip().upper()
            famNameLetters = set(famName)
            illegalChars = famNameLetters - protLetters
            #print illegalChars
            if len(illegalChars)==0 and len(famName)>5:
                #print "adding", famName
                names[famName]+=1

        if len(parts)<2:
            continue
        firstNames = parts[1:]
        #print firstNames
        for first in firstNames:
            first = first.strip().upper()
            firstLetters = set(first)
            if len(firstLetters - protLetters)==0 and len(first)>5:
                names[first]+=1

for fn, count in names.iteritems():
    if count>2:
        print fn
