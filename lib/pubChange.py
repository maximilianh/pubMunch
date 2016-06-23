import logging, optparse, os, glob, zipfile, types, gzip, shutil, sys
from os.path import *
import pubGeneric, maxRun, pubStore, pubConf, maxCommon, pubXml, pubPubmed

def filterOneChunk(inFname, searchSpec, outFname):
    """ 
    filter one chunk. searchSpec can be a list of keywords (e.g. ebola,filovirus) or 
    a filename of a list of PMIDs.
    """ 
    logging.debug("filtering %s" % inFname)
    pmids = None
    if isfile(searchSpec):
        pmids = set([int(l.strip()) for l in open(searchSpec)])
    else:
        words = searchSpec.split(",")
        words = [w.lower() for w in words]

    reader = pubStore.PubReaderFile(inFname)
    store  = pubStore.PubWriterFile(outFname)

    for article, files in reader.iterArticlesFileList(None):
        # this is the filtering part: continue if article is not accepted
        if pmids!=None:
            if (article.pmid=="" or int(article.pmid) not in pmids):
                logging.debug("skipping %s, no PMID or not in filter file" % article.pmid)
                continue
        else:
            foundMatch = False
            for w in words:
                for fileRow in files:
                    cont = fileRow.content.lower()
                    if w in cont:
                        foundMatch = True
                        break
                if foundMatch:
                    break

            if not foundMatch:
                continue

        # now write the article to output directory
        store.writeArticle(article.articleId, article._asdict())
        for fileRow in files:
            store.writeFile(article.articleId, fileRow.fileId, fileRow._asdict())
    store.close()

def submitJobs(inSpec, filterSpec, outDir):
    inDirs = pubConf.resolveTextDirs(inSpec)
    runner = pubGeneric.makeClusterRunner(__file__, maxJob=pubConf.convertMaxJob, algName=inSpec)

    outFnames = []
    for inDir in inDirs:
        inFnames = glob.glob(join(inDir, "*.articles.gz"))
        for inFname in inFnames:
            outFname = join(outDir, basename(dirname(inFname))+"-"+basename(inFname))
            outFnames.append(outFname)
            outFnames.append(outFname.replace('.articles.gz','.files.gz'))
            #command = "%s %s filterJob {check in exists %s} %s %s" % \
                #(sys.executable, __file__, inFname, pmidFname, outFname)
            runner.submitPythonFunc(__file__, "filterOneChunk", [inFname, filterSpec, outFname])
    runner.finish(wait=True)
    return outFnames

def rechunk(inDir, outDir):
    """ Read and write everything in inDir and write to outDir. potentially
    merges small chunks into bigger chunks """ 
    existOutFnames = glob.glob(join(outDir, "*"))
    assert(len(existOutFnames)<=1) # only one "parts" directory allowed
    artCount = 0
    chunkCount = 0
    store = None
    outFnames = []
    for reader in pubStore.iterPubReaders(inDir):
        for article, files in reader.iterArticlesFileList(None):
            if store==None:
                outFname = join(outDir, "0_%05d.articles" % chunkCount)
                store = pubStore.PubWriterFile(outFname)
                logging.debug("Writing to %s" % outFname)

            logging.debug("Adding %s, %d files" % (article.externalId, len(files)))
            store.writeArticle(article.articleId, article._asdict())
            for fileRow in files:
                store.writeFile(article.articleId, fileRow.fileId, fileRow._asdict())

            artCount += 1
            if artCount % pubConf.chunkArticleCount == 0:
                store.close()
                store = None
                chunkCount += 1

    if artCount % pubConf.chunkArticleCount !=0:
        outFnames.append(outFname)

    logging.info("Created %d chunks with %d article" % (chunkCount+1, artCount))
    if store!=None:
        store.close()
    pubStore.updateSqlite(outDir)

def filterCmd(inSpec, searchSpec, outSpec, options):
    outDir = pubConf.resolveTextDir(outSpec)
    assert(outDir!=None)
    maxCommon.mustBeEmptyDir(outDir)
    return submitJobs(inSpec, searchSpec, outDir)

def parseIdFname(fname):
    res = {}
    for row in maxCommon.iterTsvRows(fname):
        res[int(row.artId1)] = row.pmid
    return res
        
def updateSqliteIds(datasetString, artToPmid):
    " update the sqlite db given a list of (articleId, pmid) tuples "
    logging.info("Updating the sqlite DB %s" % datasetString)
    con, cur = pubStore.openArticleDb(datasetString)
    pmidArtIds = [(y,x) for x,y in artToPmid]
    cur.executemany("UPDATE articles SET pmid=? WHERE articleId=?", pmidArtIds)
    con.commit()

def addPmids(datasetString):
    " for a given dataset, add the pmids from the pubFingerprint output file to the article files "
    #datasetString = args[0]

    textDir = pubConf.resolveTextDir(datasetString)
    logging.info("Changing article files in %s" % textDir)
    aToPfn = join(textDir, pubConf.idFname)
    logging.info("Reading art -> pmid mapping from %s" % aToPfn)
    artToPmid = parseIdFname(aToPfn)
    fnames = glob.glob(join(textDir, "*.articles.gz"))
    logging.info("Running on %d article files" % len(fnames))
    pm = maxCommon.ProgressMeter(len(fnames), stepCount=100)
    updateSqliteIds(textDir, artToPmid.items())
    #sys.exit(0)

    logging.info("Updating tab sep files")
    for fname in fnames:
        # write headers
        newFname = join(pubConf.TEMPDIR, basename(fname))
        logging.debug("reading %s, writing %s" % (fname, newFname))
        newF = gzip.open(newFname, "w")
        newF.write(gzip.open(fname).readline())

        # write rows, replacing pmids on the way
        for row in maxCommon.iterTsvRows(fname):
            artId = int(row.articleId)
            if int(row.articleId) in artToPmid:
                row = row._replace(pmid=artToPmid[artId])
            newF.write((u'\t'.join(row)).encode("utf8"))
            newF.write("\n")
        newF.close()

        # rename old, move over the new one
        shutil.move(fname, fname+".bak")
        shutil.move(newFname, fname)
        pm.taskCompleted()


