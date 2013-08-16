# example file for pubtools
import logging, glob
from os.path import join
import geneFinder, pubConf

DICTDIR=pubConf.markerDbDir
MAXCOUNT=80

# proteins, like EC numbers, can be mapped to many genes
# we accept these mappings and create multiple annotations for an ID 
# as long as one ID is not mapped to more than X genes
MAXGENEPERID=10

# === ANNTOATOR ====
#def getSearchTypes(paramDict):
#    """ get searchType from paramDict, if not set, return all possible types,
#        possible types are defined by all second parts of bedfiles in markerDir
#    """
#    reDict = geneFinder.compileREs()
#    bedFiles = glob.glob(join(DICTDIR, "*.bed"))
#    logging.info("Found bedfiles in %s: %s" % (pubConf.markerDbDir, bedFiles))
#    #allTypes = [basename(x).split(".")[1] for x in bedFiles]
#    paramTypes = paramDict.get("searchType", "").split(",")
#    if paramTypes==[""]:
#        searchTypes = reDict.keys() # search for all types
#    else:
#        searchTypes = paramTypes
#    logging.info("Searching for: %s" % searchTypes)
#    return set(searchTypes)

class Annotate:
    def __init__(self):
        # this variable has to be defined, otherwise the jobs will not run.
        # The framework will use this for the headers in table output file
        # some identifiers are mapped to entrez gene ids directly
        # for these we put the original ID and original type into 
        # recogType and recogId
        self.headers = ["start", "end", "type", "markerId", "recogType", "recogId"]

        # let's ignore files with more than X matches
        self.MAXCOUNT = 50

        # we want sections in our tables
        self.sectioning = True
        # we don't want to run on both pdf and xml
        self.bestMain = True

    # this method is called ONCE on each cluster node, when the article chunk
    # is opened, it fills the kwDict variable
    def startup(self, paramDict):
        """ parse dictioary of keywords """
        #self.searchTypes = getSearchTypes(paramDict)
        geneFinder.initData(addOptional=True)

    # this method is called for each FILE. one article can have many files
    # (html, pdf, suppl files, etc). article data is passed in the object 
    # article, file data is in "file". For a list of all attributes in these 
    # objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
    def annotateFile(self, article, file):
        " go over words of text and check if they are in dict "
        text = file.content
        count = 0
        rows = []
        for start, end, markerType, markerId in geneFinder.findIdentifiers(text):
            # never resolve some types
            if markerType in ["band", "genbank", "refseq", "ensembl", "snp"]:
                if markerType=="snp":
                    markerId = "rs"+markerId
                row = [start, end, markerType, markerId, "", ""]
                rows.append(row)
            else:
                # try to resolve most other types to genes
                genes = geneFinder.markerToGenes(markerType, markerId)
                if genes!=None and len(genes)<MAXGENEPERID:
                    geneSyms = set(genes)
                    for geneSym in geneSyms:
                        geneId, geneSym = genes.items()[0]
                        row = [start, end, "gene", geneSym, markerType, markerId]
                        rows.append(row)
                else:
                    row = [start, end, markerType, markerId, "", ""]
                    rows.append(row)
            
        if len(rows)>MAXCOUNT:
            logging.info("more than %d annotations (e.g. excel table), skipping file" % MAXCOUNT)
            return None
        return rows
            
# === MAP/REDUCE TASK ====
class FilterKeywords:
    def __init__(self):
       self.headers = ["keyword"] 

    def startup(self, paramDict, resultDict):
        self.maxCount = paramDict["maxCount"]
        kwFilename = paramDict["keywords"]
        self.searchTypes = getSearchTypes(paramDict)
        geneFinder.initData(self.searchTypes, addOptional=True)

    def map(self, article, file, text, resultDict):
        matches = list(geneFinder.findIdentifiers(text))
        for start, end, type, word in matches:
            resultDict.setdefault(word, set()).add(file.fileId)

    # this is called after all jobs are finished, on the main machine
    # it is called once for each key
    def reduce(self, word, fileIds):
        if len(fileIds) < self.maxCount:
            return word, len(fileIds)

if __name__=="__main__":
    import doctest
    doctest.testmod()


