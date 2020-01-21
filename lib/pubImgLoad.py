from __future__ import print_function
# extract images from PDFs and load them into a sqlite database

from os.path import *
import os, sys, logging, shutil, subprocess, hashlib
import tempfile
import glob

try:
    from pysqlite2 import dbapi2 as sqlite3
except:
    import sqlite3
    logging.warn("python's default sqlite3 is slower. do 'pip install pysqlite' to get the update")

import pubConf, maxCommon, pubGeneric

# width of thumbnails
WIDTH = 150

def pbmSize(fh):
    line1 = fh.readline()
    line2 = fh.readline()
    x, y = line2.split()
    x, y = int(x), int(y)
    return x, y

def looksInteresting(x, y):
    " check if resolution and ratio of PPM make sense to keep "
    x = float(x)
    y = float(y)
    ratio = abs(x/y)
    logging.log(5, "size %d x %d, ratio %f" % (x, y, ratio))
    if ratio < 0.05 or ratio > 20: # 20 times higher than wide or wider than high -> fishy
        logging.log(5, "skipping image")
        return False
    if x < 100 or y < 100: # too small -> useless
        logging.log(5, "keeping image")
        return False
    return True

def makeMd5(blob):
    " return base64 encoded hash of binary data "
    m = hashlib.md5()
    m.update(blob)
    return m.digest().encode('base64').strip()

def pngDimensions(fname):
    " return width, height of png. does not work with jpg. "
    cmd = ["/usr/bin/file",  fname]
    logging.debug(cmd)
    output = subprocess.check_output(cmd)
    #/cluster/home/max/tmp.png: PNG image data, 589 x 553, 8-bit/color RGB, non-interlaced
    logging.debug(output)
    fs = output.split()
    w = int(fs[4])
    h = int(fs[6].strip(","))
    return w, h

md5Blacklist = None

def loadBlacklist():
    " load blacklisted MD5 sums, appear > 3 times in crawl from July 2016 "
    global md5Blacklist
    if md5Blacklist!=None:
        return

    fname = join(pubConf.staticDataDir, "imgExtract", "blacklistMd5.txt")
    logging.debug("Reading %s" % fname)
    md5Blacklist = set(open(fname).read().splitlines())
    logging.debug("Got %d blacklisted MD5 sums, e.g. %s" % (len(md5Blacklist), str(list(md5Blacklist)[:10])))
    
def getImages(pdfName):
    """ returns a list of tuples 
    (imgId (int), isThumbnail (int), width, height, md5sum, PNGBinarydataBlob) extracted from pdfName.
    returns two tuples per image, one is the original, one is the thumbnail.
    """
    loadBlacklist()

    head = open(pdfName).read(30)
    if "<html" in head or "<HTML" in head:
        logging.info("PDF %s is an HTML file, skipping" % pdfName)
        return None

    logging.debug("Extracting images from %s" % pdfName)
    tempDir = tempfile.mkdtemp(prefix="pdfimages", dir=pubConf.getTempDir())
    maxCommon.delOnExit(tempDir)
    outStem = join(tempDir, "img")
    cmd = "pdfimages %s %s" % (pdfName, outStem)
    maxCommon.runCommand(cmd)

    # convert to png
    data = []
    imgId = 0
    for fname in glob.glob(join(tempDir, "*.ppm")):
        logging.debug("got image %s" % fname)
        x, y = pbmSize(open(fname))
        if not looksInteresting(x, y):
            logging.debug("Image is too small or too long/wide")
            continue

        logging.debug("Loading image into sqlite")
        outFname = "%s.png" % fname
        cmd = "convert %s %s" % (fname, outFname)
        maxCommon.runCommand(cmd)
        
        pngBlob = open(outFname).read()
        md5Str = makeMd5(pngBlob)

        print("XX", md5Str, list(md5Blacklist)[:10])
        if md5Str in md5Blacklist:
            logging.debug("Image MD5 is blacklisted")
            continue

        data.append( (imgId, 0, x, y, md5Str, pngBlob) )

        # make the thumbnail
        thumbFName = "%s.thumb.png" % fname
        # see https://www.smashingmagazine.com/2015/06/efficient-image-resizing-with-imagemagick/
        # but can't use -posterize 136 on centos6
        cmd = "convert -filter Triangle -define filter:support=2 -thumbnail %d " \
            "-unsharp 0.25x0.25+8+0.065 -dither None -quality 82 -define png:compression-filter=5 " \
            "-define png:compression-level=9 -define png:compression-strategy=1 " \
            "-define png:exclude-chunk=all -interlace none -colorspace " \
            "sRGB -strip %s %s" % (WIDTH, fname, thumbFName)
        maxCommon.runCommand(cmd)

        x, y = pngDimensions(thumbFName)
        pngBlob = open(thumbFName).read()
        md5Str = makeMd5(pngBlob)

        data.append( (imgId, 1, x, y, md5Str, pngBlob) )

        imgId += 1
            
    shutil.rmtree(tempDir)
    maxCommon.ignoreOnExit(tempDir)
    return data

def createImgTable(cur):
    " create the table for the images "
    try:
        cur.execute('CREATE TABLE img (title varchar(65535), authors varchar(65535), journal varchar(65535), year varchar(255), '
            'pmid varchar(255), doi varchar(255), pmcId varchar(255), '
            'fileType varchar(255), desc varchar(255), url varchar(8000), fileId varchar(255), '
            'imgId int, isThumb int, width int, height int, md5 varchar(255), imgSize int, data BLOB)'
            )
    except sqlite3.OperationalError:
        pass

def addIndexes(cur):
    " add the indexes to the img table "
    logging.info("Adding indexes to image sqlite db")
    query = "CREATE INDEX pmidIdx ON img (pmid);"
    cur.execute(query)
    query = "CREATE INDEX pmcIdIdx ON img (pmcId);"
    cur.execute(query)
    query = "CREATE INDEX doiIdx ON img (doi);"
    cur.execute(query)
    query = "CREATE INDEX md5Idx ON img (md5);"
    cur.execute(query)

def imgDbFname(articlePath):
    " return name of img db file "
    imgDbPath = articlePath.replace(".articles.gz", "").replace(".files.gz", "")+".img.db"
    return imgDbPath

def openImgDbForArticles(articlePath):
    " given the path of the articles.gz file, e.g. xxx/yyy/0_00001.articles.gz, create the img db "
    imgDbPath = imgDbFname(articlePath)
    con = sqlite3.connect(imgDbPath)
    createImgTable(con)
    return con

def loadImages(con, artDict, fileDict):
    " load all relevant images from PDF into an sqlite db "
    pdfData = fileDict["content"]
    tmpFile, tmpPdfName = pubGeneric.makeTempFile("pubImgLoad", ".pdf")
    tmpFile.write(pdfData)
    tmpFile.flush()

    imgId = 0
    title = artDict.get("title", "")
    authors = artDict.get("authors", "")
    journal = artDict.get("journal", "")
    year = artDict.get("year", "")
    pmid = artDict.get("pmid", "")
    doi = artDict.get("doi", "")
    pmcId = artDict.get("pmcId", "")

    fileType = fileDict["fileType"]
    desc = fileDict["desc"]
    url = fileDict["url"]
    fileId = str(fileDict["fileId"])

    imgRows = getImages(tmpPdfName)
    if imgRows is None:
        tmpFile.close()
        return

    for imgId, isThumbnail, width, height, md5, pngData in imgRows:
        logging.debug("Adding image %d" % imgId)
        size = len(pngData)
        fileInfo = [ title, authors, journal, year, pmid, doi, pmcId, fileType, \
                desc, url, fileId, imgId, isThumbnail, width, height, md5, size, buffer(pngData) ]
        qStr = ",".join( (["?"] * len(fileInfo)) )
        con.execute('INSERT INTO img values (%s)' % qStr, fileInfo)
        con.commit()

    tmpFile.close() # = delete
