import pubDnaFind, orgDetect, pubConf

def findDbs(text):
    """ find species in text """
    for genome, keywords in pubConf.speciesNames.iteritems():
        for keyword in keywords:
            start = text.find(keyword)
            if start==-1:
                continue
            #end   = start+len(keyword)
            #yield [start, end, genome]
            yield genome
            break 
            # we stop once we have detected one species name for each db
            # no need to find the others

""" interface to pubtools to pubDnaFind """
class Annotate:
    def __init__(self):
        self.headers = ["start", "end", "seq", "partCount", "tainted", "dbs"]
        #self.orgDetect = orgDetect.OrgDetect()

    def annotateFile(self, articleData, fileData):
        """ interface for pubRun to get annotation lines for text """
        # find organisms in text
        dbs = set()
        text = fileData.content
        dbString = ",".join( set(findDbs(text)))

        # find dna in text and add organisms
        for row in pubDnaFind.nucleotideOccurrences(text):
            if row.seq=="": # can only happen if seq is a restriction site
                continue
            row = [str(x) for x in row]
            row.append(dbString)
            yield row

