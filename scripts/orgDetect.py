# This is the python module that recognized species names in text
# It is used by t2gDnaDetect

from pubConf import *

class OrgDetect:
    """ find species names in text"""
    def __init__(self):
        self.headers = ["start", "end", "db"]

    def annotRows(self, text):
        """ interface for pubRun to get annotation lines for text """
        for genome, keywords in speciesNames.iteritems():
            for keyword in keywords:
                start = text.find(keyword)
                if start==-1:
                    continue
                end   = start+len(keyword)
                yield [start, end, genome]
                break # we stop once we have detected a genome

# ----- 
if __name__ == "__main__":
    import doctest
    doctest.testmod()

