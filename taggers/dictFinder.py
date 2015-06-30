# search for a dictionary compiled with pubDictCompile
import os, logging
from fastFind import fastFind, loadLex
import pubNlp
from os.path import basename

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "matches", "pos", "snippet"]

preferXml = True

# dictionaries
lexes = {}

# convert all text to lowercase before matching against dict ?
toLower = True

# require strings
reqStrings = None

# this method is called once on each cluster node upon startup
def startup(paramDict):
    """ parse file into lexicons """
    global lexes
    for fname in paramDict["fnames"].split(","):
        lexName = basename(fname).split(".")[0]
        lexes[lexName]=loadLex(fname)

    if "toLower" in paramDict:
        global toLower
        toLower = bool(int(paramDict["toLower"]))
        logging.info("toLower is %s" % toLower)

    if "reqStrings" in paramDict:
        global reqStrings
        reqStrings = paramDict["reqStrings"].split(",")

def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    if len(text)>100000:
        return

    if reqStrings!=None:
        found = False
        #sentLower = sent.lower()
        textLower = text.lower()
        for rs in reqStrings:
            if text.find(rs)!=-1:
            #if sentLower.find(rs)!=-1:
                found = True
                break
        if not found:
            return

    for section, sentStart, sentEnd, sent in pubNlp.sectionSentences(text, file.fileType, mustHaveVerb=False):
        #if len(sent)<20:
            #logging.debug("Sentence too short: %d characters" % len(text))
            #continue
        #if len(sent)>2000:
            #logging.debug("Sentence too long: %d characters" % len(text))
            #continue

        found = True
        posList = []
        allMatches = []
        for lexName, lex in lexes.iteritems():
            matches = []
            lexMatches = fastFind(sent, lex, toLower=toLower)
            if len(lexMatches)==0 or len(lexMatches)>10:
                found = False
                break
            for start, end, word in lexMatches:
                matches.append(word.replace("="," ").replace(",", " "))
                posList.append("%d-%d" % (start, end))
            allMatches.append("%s=%s" % (lexName, ",".join(matches)))
        if found:
            yield sentStart, sentEnd, "|".join(allMatches), "|".join(posList), sent

        #logging.info("%d gene matches in file (>10), skipping article %s/file %s, fileType %s" % (len(rows), article.externalId, file.fileId, file.fileType))

if __name__=="__main__":
    import doctest
    doctest.testmod()
