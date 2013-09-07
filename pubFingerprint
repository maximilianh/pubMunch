#!/usr/bin/env python

# try to infer PMIDs by comparing issn/vol/issue/page against medline

# load default python packages
import logging, optparse, sys, os, marshal, unicodedata
from os.path import *

# add <scriptDir>/lib/ to package search path
sys.path.insert(0, join(dirname(abspath(__file__)), "lib"))

import pubGeneric, pubConf, maxCommon, pubCompare, pubStore, pubChange

def main(args, options):
    step, dataSet1 = args[:2]
    pubGeneric.setupLogging("", options)
    updateIds = options.updateIds

    if step=="testDoi":
        pf = pubCompare.PmidFinder()
        ad = pubStore.createEmptyArticleDict()
        ad["doi"] = dataSet1
        print "PMID:", pf.lookupPmid(ad)
        sys.exit(0)

    dir1 = pubConf.resolveTextDir(dataSet1)

    if step=="index":
        pubCompare.createWriteFingerprints(dir1, updateIds)

    elif step=="lookup":
        dataSet2 = args[2]
        dir2 = pubConf.resolveTextDir(dataSet2)
        outputFname = join(dir2, "%s.ids.tab" % dataSet1)
        noMatchFname = join(dir2, "%s.ids.noMatch.tab" % dataSet1)
        noPrintFname = join(dir2, "%s.ids.noFingerprint.tab" % dataSet1)

        # read fingerprints from dir1
        mapStoreFname = join(dir1, "fingerprints.marshal")
        logging.info("Reading %s" % mapStoreFname)
        ifh = open(mapStoreFname)
        map0, map1, map2, artIds = marshal.load(ifh)

        ofh = open(outputFname, "w")
        headers = ["fingerprint", "artId1", "extId1", "doi1", "articleId", "extId", "doi", "pmid"]
        ofh.write("\t".join(headers))
        ofh.write("\n")

        noPrints = []
        noMatches = []
        for artData in maxCommon.iterTsvDir(dir2, ext=".articles.gz"):
            artIdTuple = pubCompare.lookupArtIds(artData, map0, map1, map2, artIds, noPrints, noMatches)
            if artIdTuple==None:
                continue

            matchFprint, artId, extId, doi, pmid = artIdTuple
            row = (matchFprint, artData.articleId, artData.externalId, \
                   artData.doi, artId, extId, doi, pmid)
            row = [unicode(x) for x in row]
            ofh.write("\t".join(row).encode("utf8"))
            ofh.write("\n")

        ofh.close()

        # write the non-matching articles to two files
        mfh = open(noPrintFname, "w")
        for p in noPrints:
            mfh.write("\t".join(p).encode("utf8")+"\n")
        mfh = open(noMatchFname, "w")
        for m in noMatches:
            mfh.write("\t".join(m).encode("utf8")+"\n")

        logging.info("No fingerprint for %d articles, no match %d" % (len(noPrints), len(noMatches)))
        logging.info("Results written to %s" % outputFname)
        logging.info("non-matching articles written to %s and %s" % (noPrintFname, noMatchFname))

        logging.info("now adding PMIDs to tab-sep files")
        pubChange.addPmids(dataSet2)

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <command> <sourceDataset> <targetDataset> - map between datasets by comparing fingerprints of papers. Source is usually Medline.

step can either be 'index' or 'lookup' or for testing 'testdoi' with a DOI

'index' will create a file fingerprints.marshal in the source directory and also some dbms.
'lookup' will read this file and create a file medline.ids.tab in target directory.
'testdoi': to lookup a single doi or other fingerprint

medline.ids.tab lists matching articles from both sets in sourceDataset

examples:
pubFingerprint index medline
pubfingerprint lookup medline elsevier


When compared to data from crossref and a random collection of 10.000 DOIs, achieves a precision
of 99.99%.

""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages")
parser.add_option("-u", "--updateIds", dest="updateIds", action="append", help="updateIds to process for the 'index' command, can be specified multiple times")
#parser.add_option("-s", "--wordList", dest="wordList", action="store", help="optional list of words to use")
(options, args) = parser.parse_args()

if args==[]:
    parser.print_help()
    exit(1)

main(args, options)
