import logging, optparse, os, glob, zipfile, types, gzip, shutil, sys
from os.path import *
import pubGeneric, maxRun, pubStore, pubConf, maxCommon, pubXml, pubPubmed

def filterOneChunk(inFname, pmidFname, outFname):
    """ 
    filter one chunk
    """ 
    pmids = set([int(l.strip()) for l in open(pmidFname)])
    reader = pubStore.PubReaderFile(inFname)
    store  = pubStore.PubWriterFile(outFname)
    for article, files in reader.iterArticlesFileList():
        if article.pmid=="" or int(article.pmid) not in pmids:
            logging.debug("skipping %s, no PMID or not in filter file" % article.pmid)
            continue
        store.writeArticle(article.articleId, article._asdict())
        for fileRow in files:
            store.writeFile(article.articleId, fileRow.fileId, fileRow._asdict())
    store.close()

def submitJobs(inSpec, pmidFname, outDir):
    inDirs = pubConf.resolveTextDirs(inSpec)
    runner = pubGeneric.makeClusterRunner(__file__, maxJob=pubConf.convertMaxJob, algName=inSpec)
    pmidFname = os.path.abspath(pmidFname)

    for inDir in inDirs:
        inFnames = glob.glob(join(inDir, "*.articles.gz"))
        for inFname in inFnames:
            outFname = join(outDir, basename(inFname))
            command = "%s %s filterJob {check in exists %s} %s %s" % \
                (sys.executable, __file__, inFname, pmidFname, outFname)
            runner.submit(command)
    runner.finish(wait=True)

def rechunkCmd(args, options):
    #reader = pubStore.PubReaderFile(inFname)
    #artCount = 0
    #chunkCount = 0
    #logging.debug("Writing to %s" % outFname)
    #store = pubStore.PubWriterFile(join(outDir, "0_00000.articles.gz"))
    #print "Directory: %s" % inDir
    #pm = maxCommon.ProgressMeter(len(inFnames))
    #artCount += 1
    #if artCount % pubConf.chunkArticleCount == 0:
    pass
        #store.close()
        #chunkCount += 1
        #store = pubStore.PubWriterFile(join(outDir, "0_%05d.articles.gz" % chunkCount))
        #logging.info("Accepting %s, %d files" % (article.externalId, len(files)))
        #store.writeArticle(article.articleId, article._asdict())
        #for fileRow in files:
            #store.writeFile(article.articleId, fileRow.fileId, fileRow._asdict())
            #pm.taskCompleted()
    #store.close()

def filterCmd(args, options):
    inSpec, pmidFname, outSpec = args
    outDir = pubConf.resolveTextDir(outSpec, makeDir=True)
    assert(outDir!=None)
    maxCommon.mustBeEmptyDir(outDir)
    submitJobs(inSpec, pmidFname, outDir)

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


