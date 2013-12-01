# utility, wrappers, convenience and helper functions 
# some are dna related

import random
from sys import *
import os, logging
import re
import shutil, tarfile
import math, socket
import urllib
import urllib2
from math import *
import ftplib
import unicodedata
import array, copy, posixpath
from posixpath import curdir, sep, pardir, join

try:
    import simplexmlparse
except:
    pass

try:
        import warnings
        warnings.filterwarnings("ignore",category=DeprecationWarning)
        import scipy.stats
        import dist # binomial functions from jacques van helden's lab
except:
    pass

try:
    import MySQLdb
except:
    pass

# for compat with mac os tiger and redhat 8, uncomment the following two lines
# import sets
# set = sets.Set

# max' libs
import util

LOGLEVEL=0 # the lower the more messages you'll see

# empty class, you can add attributes as you like during run-time, you can also print it
class Object:
    def __repr__(self):
        lines = []
        for i in self.__dict__:
            lines.append("%s: "%i+str(self.__dict__[i]))
        return "\n".join(lines)

# ----------- CONVENIENCE ------------------
def relpath(path, start=curdir):
    """Return a relative version of a path, backport to python2.4 from 2.6"""
    """http://www.saltycrane.com/blog/2010/03/ospathrelpath-source-code-python-25/ """
    if not path:
        raise ValueError("no path specified")
    start_list = posixpath.abspath(start).split(sep)
    path_list = posixpath.abspath(path).split(sep)
    # Work out how much of the filepath is shared by start and path.
    i = len(posixpath.commonprefix([start_list, path_list]))
    rel_list = [pardir] * (len(start_list)-i) + path_list[i:]
    if not rel_list:
        return curdir
    return join(*rel_list)

def extractTar(tarObject, path="."):
    """ backport from 2.5 for earlier python versions"""
    directoryInfos = []

    filenames = []
    directories = []

    members = tarObject.getmembers()
    for tarinfo in members:
        if tarinfo.isdir():
            # Extract directories with a safe mode.
            directoryInfos.append(tarinfo)
            tarinfo = copy.copy(tarinfo)
            tarinfo.mode = 0700
        tarObject.extract(tarinfo, path)

        if tarinfo.isfile():
            filenames.append(os.path.join(path, tarinfo.name))
        if tarinfo.isdir():
            directoryInfos.append(tarinfo)

    # Reverse sort directories.
    directoryInfos.sort(lambda a, b: cmp(a.name, b.name))
    directoryInfos.reverse()

    # Set correct owner, mtime and filemode on directories.
    for tarinfo in directoryInfos:
        dirpath = os.path.join(path, tarinfo.name)
        tarObject.chown(tarinfo, dirpath)
        tarObject.utime(tarinfo, dirpath)
        tarObject.chmod(tarinfo, dirpath)
        directories.append(os.path.join(path, tarinfo.name))

    return directories, filenames
 

def revComp(seq):
    table = { "a":"t", "A":"T", "t" :"a", "T":"A", "c":"g", "C":"G", "g":"c", "G":"C", "N":"N", "n":"n", 
            "Y":"R", "R" : "Y", "M" : "K", "K" : "M", "W":"W", "S":"S",
            "H":"D", "B":"V", "V":"B", "D":"H", "y":"r", "r":"y","m":"k",
            "k":"m","w":"w","s":"s","h":"d","b":"v","d":"h","v":"b","y":"r","r":"y" }
    newseq = []
    for nucl in reversed(seq):
       newseq += table[nucl]
    return "".join(newseq)

def resolveIupac(seq):
    seq=seq.upper()
    table = { "Y" : "TC", "R" : "GA", "M" : "AC", "K" : "GT", "S" : "GC", "W" : "AT", "H" : "ACT", "B" : "GTC", "V" : "GCA", "D" : "GAT", "N" : "ACTG"}
    newseq = []
    for nucl in seq:
       if nucl in table:
           newseq += "[%s]" % table[nucl]
       else:
           newseq += nucl
    newstr = "".join(newseq)
    #newstr = newstr.replace("N", "[ACTGN]")
    return newstr

def safeGet(dict, key):
    if key in dict:
        return dict[key]
    else:
        log(1, "could not find key %s in dictionary" % key)
        return None

class Logger:
    def __init__(self, baseName=None, baseDir="log", prefix="", prefixWithHost=False):
        if prefixWithHost:
            prefix = socket.gethostname()+"."+prefix
        if baseName=="stderr":
            self.of = stderr
        elif baseName==None:
            fname = prefix + os.path.splitext(os.path.split(argv[0])[1])[0] + ".log"
            if not os.path.isdir(baseDir):
                #stderr.write("error: Logging facilities of this program require a directory %s for the logfiles\n"%baseDir)
                #exit()
                stderr.write("info: no log dir specified, logging to stderr")
                of = stderr
            logfname = os.path.join(baseDir, fname)
            if os.path.exists(logfname):
                os.rename(logfname, logfname+".old")
            self.of = open(logfname, "w")
        else:
            self.of = open(baseName, "w")

    def log(self, line, toStderr=False, onlyStderr=False):
        if not onlyStderr:
            self.of.write(line+"\n")
        if toStderr or onlyStderr:
            stderr.write(line+"\n")

def log(level, text):
    # 0 = never suppress
    # 1 = warning
    # 2 = info
    # 3 = debug
    prefix = ""
    if level >= LOGLEVEL:
        if level == 1:
            prefix = "warning:"
        if level == 2:
            prefix == "info:"
        if level >= 3:
            prefix == "debug:"

        stderr.write(prefix+"%s\n" % text)

def error(text):
    stderr.write("error: %s\n" % text)
    exit(1)

def execCmdLine(cmdLine, progName=""):
    logging.debug("Running %s" %cmdLine)
    ret = os.system(cmdLine)
    if ret==None or ret!=0:
        logging.error("error while running this command: "+cmdLine)
        exit(1)

def highlightOccurence(string, searchStr):
    pos = string.lower().find(searchStr.lower())
    endPos = pos+len(searchStr)
    if pos==-1:
        return None
    tag = "--"
    string = string[:pos] + tag + string[pos:endPos] + tag + string[endPos:]
    return string

def readAniseed(asAnnot, tissueKeywords, bgGenesFile=None):
     # read annotation, tissueKeywords is list of keywords that have to be found for a gene
     # asAnnot is dict of lists, as returned by slurpdictlist
    targetGenes = set()
    bgGenes = set()
    asBgGenes = set()
    for annot, genes in asAnnot.iteritems():
        asBgGenes.update(genes)
        for kw in tissueKeywords:
            if annot.find(kw)!=-1:
                targetGenes.update(genes)
        bgGenes.update(genes)
    #stderr.write("Found %d target genes in file %s\n" % (len(targetGenes), asFile))
    if bgGenesFile:
        bgGenes = set(tabfile.slurplist(bgGenesFile, field=0))
        #stderr.write("Found %d background genes in file %s\n" % (len(bgGenes), bgGenesFile))
    else:
        bgGenes=asBgGenes
    return targetGenes, bgGenes

# ------------- HTML STUFF ----------------------------------
def parseXml(template, string):
    parser = simplexmlparse.SimpleXMLParser( template )
    obj = parser.parse(string)
    return obj

def parseXmlFile(template, fname):
    xml = open(fname).read()
    return parseXml(template, xml)

def parseXmlUrl(template, url):
    xml = util.httpGet(url).read()
    return parseXml(template, xml)

def openFtpConn(host, path, user, password):
    """ returns ftp connection object, does not support proxies anymore """
    # format for ftp_proxy http://updateproxy.manchester.ac.uk:3128
    #ftpProxyString=os.environ.get("ftp_proxy")

    #if ftpProxyString==None:
        #ftp = FTP()           # connect to host, default port
        #ftp.connect(host)
        #ftp.login()               # user anonymous, passwd anonymous@
    #else:
    ftp = ftplib.FTP()
    #port = int(ftpProxyString.split(":")[2])
    #proxyHost = ftpProxyString.split("//")[1].split(":")[0]
    #print "using proxy %s, port %d" % (proxyHost, port)
    #print "connecting to host %s" % (host)
    #ftp.connect(proxyHost, port, 5)
    #ftp.login("anonymous@%s" % host, "maximilianh@gmail.com")
    ftp.connect(host)
    ftp.login(user, password)
    ftp.cwd(path)
    logging.debug("ftp connect: %s, %s, %s, %s" % (host, path, user, password))
    #print "ok"
    return ftp

def getFtpDir(ftp, dir, onlySubdirs=False):
    """ return urls of directories in ftp-folder, needs a ftp connection object"""
    #print dir
    try:
        ftp.cwd(dir)
    except:
        print ("error: directory %s does not seemt to exist on host %s" % (dir, ftp.host))
        return None
    lines = []
    dirs = [] 
    ftp.retrlines('LIST', lines.append)     # list directory contents
    for l in lines:
        if onlySubdirs and not l.startswith("d"):
            continue
        fs = l.split()
        subdir = fs[8]
        dirs.append(os.path.join(dir, subdir))
    return dirs

def ftpDownload(ftp, filename, locPath):
    logging.debug("Downloading %s via ftp to %s" % (filename, locPath))
    try:
        ftp.retrbinary("RETR "+filename, open(locPath, "wb").write)
    except ftplib.error_perm:
        return False
    return True

# -- for httpGet, helper class for redirects ---
class SmartRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_301(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_301(
            self, req, fp, code, msg, headers)
        result.status = code
        return result

    def http_error_302(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_302(
            self, req, fp, code, msg, headers)
        result.status = code
        return result

def httpGet(url):
    req = urllib2.Request(url)
    opener = urllib2.build_opener(SmartRedirectHandler())
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.7.8) Gecko/20050524 Fedora/1.5 Firefox/1.5')
    f = opener.open(req, timeout=5)
    return f

def httpMatches(url, reStr):
    """ return matches for given regex, use () around the part that you want to extract """
    html = httpGet(url)
    html= html.readlines()
    regex = re.compile(reStr)
    matches = []
    for l in html: 
        matches.extend(regex.findall(l))
    return matches

def httpDownload(url, fname, verbose=False):
    if verbose:
        logging.info("Downloading %s to %s" % (url, fname))
    try:
        fh = httpGet(url)
    except urllib2.URLError:
        logging.error("%s does not exist" % url)
        return False

    tryCount = 10
    success = False
    while tryCount>0 and not success:
        try:
            data = fh.read()
            success = True
        except socket.timeout:
            logging.info("Retrying download of %s" % url)
            tryCount =- 1
            pass
            
    if not success:
        logging.error("Unable to download %s" % url)
        return False

    open(fname, "wb").write(data)
    return True

def htmlHeader(fh, title):
    fh.write("<html>\n<head>\n")
    fh.write("<title>"+title+"</title>\n</head>")
    fh.write("<body>\n")

def htmlFooter(fh):
    if fh!=None:
        fh.write("</body>\n")
        fh.write("</html>")

def htmlH3(str):
        fh.write("<h3>" + str + "</h3>\n")

def htmlLink(fh, text, url):
    if fh!=None:
        fh.write("<a href=\"" + url + "\">" + text + "</a>\n")

def htmlAnchor(fh, name):
    if fh!=None:
        fh.write("<a name=\"" + name + "\">\n")

def htmlAnchor(fh, name):
    if fh!=None:
        fh.write("<a name=\"" + name + "\">\n")

def htmlPre(fh, text):
    if fh!=None:
        fh.write("<pre>\n" + text + "</pre>\n")

def htmlH4(fh, text):
    if fh!=None:
        fh.write("<h4>\n" + text + "</h4>\n")

def htmlH3(fh, text):
    if fh!=None:
        fh.write("<h3>\n" + text + "</h3>\n")

def htmlRuler(fh):
    if fh!=None:
        fh.write("<hr>\n")

# ---------- MYSQL STUFF --------------------------------

def dbConnect(user, pwd, db, port=3306, host="localhost"):
     db = MySQLdb.connect(host, user, pwd, db, port=port)
     return db

def sql(db, sql, fields=None):
    cursor = db.cursor()
    if fields:
        cursor.execute(sql, fields)
    else:
        cursor.execute(sql)
    rows = cursor.fetchall()

    # check if any column is an array (bug in mysqldb 1.2.1 but not in 1.2.2)
    if len(rows)>0:
        arrayType = type(array.array('c', "a"))
        needConvert = False
        row1 = rows[0]
        for d in row1:
            if type(d)==arrayType:
                needConvert=True
                break
        if needConvert:
            newRows = []
            for row in rows:
                newCols = []
                for col in row:
                    if type(col)==arrayType:
                       newCols.append(col.tostring()) 
                    else:
                        newCols.append(col)
                newRows.append(newCols)
            rows = newRows
        # end bugfix

    cursor.close ()
    db.commit()
    return rows

# ------------- ALIGNING/FILTERING SEQUENCES (Wrappers, more or less) --------------------------------

def getPrimers(fileSuffix, faSeqString, maxProductSize, optSize, targetStart, targetEnd, minTm, oTm, maxTm, minGc, oGc, maxGc):
        """ return primers from primer3 in format [primer1, primer2] where primer1/2 = (tm, gc, seq) (tm, gc,seq) """
        seqfile = TEMPDIR + fileSuffix + ".fa"
        f = open(seqfile , "w")
        f.write(faSeqString)
        f.close()

        tmpFile  = TEMPDIR + fileSuffix + ".primer3"
        cmdLine = "eprimer3 %s -firstbaseindex 0 -productosize %d -target %d,%d -mintm %d -otm %d -maxtm %d -mingc %d -ogcpercent %d -maxgc %d -productsizerange 50-%d -out %s" % (seqfile,  optSize, targetStart, targetEnd, minTm, oTm, maxTm, minGc, oGc, maxGc, maxProductSize, tmpFile)
        execCmdLine(cmdLine, "primer3")

        f = open(tmpFile, "r")
        primers = []
        for l in f:
            l = l.strip()
            fs = l.split()
            if len(fs)>5 and fs[1]=="PRIMER":
                tm = fs[4]
                gc = fs[5]
                seq = fs[6]
                if fs[0]=="FORWARD":
                    curPrimer = [ (seq,tm,gc)]
                else:
                    curPrimer.append( (seq, tm, gc) )
                primers.append(curPrimer)

        f.close()
        return primers

# ------------------- SOME BASIC STATISTICS STUFF ---------------------

class Stats:
    pass

    def __repr__(self):
        lines = []
        lines.append("TP   %f" % self.TP)
        lines.append("FP   %f"  % self.FP)
        lines.append("TN   %f"  % self.TN)
        lines.append("FN   %f"  % self.FN)
        lines.append("Sens %f"  % self.Sens)
        lines.append("Spec %f"  % self.Spec)
        lines.append("PPV  %f"  % self.PPV)
        lines.append("CC   %f"  % self.CC)
        return "\n".join(lines)

def hitStats(all, predicts, targets, notTargets=None, notPredicts=None):
        #stats = trueFalsePositives(all, predicts, targets)
        def divide(top, bottom):
            if bottom!=0:
                return float(top) / float(bottom)
            else:
                return 0.0

        if notPredicts==None:
            notPredicts = all.difference(predicts)
        if notTargets==None:
            notTargets = all.difference(targets)

        #assert(len(notTargets)!=0)
        assert(len(targets)!=0)

        stats = Stats()
        TP = len(targets.intersection(predicts))
        FP = len(notTargets.intersection(predicts))
        TN = len(notTargets.intersection(notPredicts))
        FN = len(targets.intersection(notPredicts))

        #TP = float(TP) 
        #FP = float(FP)
        #TN = float(TN)
        #FN = float(FN)

        stats.TP = TP
        stats.FP = FP
        stats.TN = TN
        stats.FN = FN

        stats.Sens = divide(TP , (TP + FN))
        stats.Spec = divide(TN , (TN + FP))
        stats.PPV  = divide(TP , (TP + FP))      # PRECISION aka Positive Predictive Value
        # Precision measures the proportion of the claimed true functional sites that are indeed true functional sites.
        stats.PC   = divide(TP , (TP + FP) )
        # Accuracy  measures the proportion of predictions, both for true functional sites and false functional sites that are correct. 
        stats.Acc  = divide((TP + TN) , (TP + FP + FN + TN))

        CC_top = TP * TN - FN * FP
        CC_bottom = math.sqrt((TP+FN)*(TN+FP)*(TP+FP)*(TN+FN))
        stats.CC = divide( CC_top , CC_bottom )

        return stats


# Binomial coefficients.
def choose(n, k):
    """binc(n, k): Computes n choose k."""
    if (k > n): return 0
    if (k < 0): return 0
    if (k > int(n/2)):
        k = n - k

    rv = 1
    for j in range(0, k):
        rv *= n - j
        rv /= j + 1
    return int(rv)

def hypergProb(k, N, m, n):
    """ Wikipedia: There is a shipment of N objects in which m are defective. The hypergeometric distribution describes the probability that in a sample of n distinctive objects drawn from the shipment exactly k objects are defective. """
    #return float(choose(m, k) * choose(N-m, n-k)) / choose(N, n)
    hp = float(scipy.comb(m, k) * scipy.comb(N-m, n-k)) / scipy.comb(N, n)
    if scipy.isnan(hp):
        stderr.write("error: not possible to calculate hyperg probability in util.py for k=%d, N=%d, m=%d, n=%d\n" %(k, N, m,n))
        stdout.write("error: not possible to calculate hyperg probability in util.py for k=%d, N=%d, m=%d, n=%d\n" %(k, N, m,n))
    return hp

# --------------------------------------------------------------------------------------------------------------------


def hypergProbSum(k, N, m, n):
    """ calculate hypergeometric probability from 0 up to a certain value k, k IS NOT INCLUDED!!"""
    """ result can be compared with R: sum(dhyper(x=42:1518, m=129, n=(1518-129), k=125)) """
    """ if 1518 genes, 129 in foreground, 125 predicted genes and overlap of 42 """
    """ in R this is 2.204418e-17"""
    sum=0.0
    for i in range(0, k):
        sum += hypergProb(i, N, m, n)
    return sum
    
def factorial(n, _known=[1]):
    assert isinstance(n, int), "Need an integer. This isn't a gamma"
    assert n >= 0, "Sorry, can't factorilize a negative"
    assert n < 1000, "No way! That's too large"
    try:
        return _known[n]
    except IndexError:
        pass
    for i in range(len(_known), n+1):
        _known.append(_known[-1] * i)
    return _known[n]

def poissProb(n, p, k):
    """ binomial probability: n number of objects, p = probability of success, k = number of trials """
    l = n*p
    #stderr.write("poissProb: n=%d, p=%f, k=%d\n" % (n,p,k))
    return (exp(-l)*l**k) / factorial(k)

def binProb(n, p, k):
    """ binomial probability: n number of objects, p = probability of success, k = number of trials """
    #return choose(n, k) * p**k * (1-p)**(n-k)
    return scipy.stats.distributions.binom.pmf(k-1, n, p)

def binProbGt(k, size, prob):
    """ binomial probability that x is > k (up to n). The corresponding R code for this is: pbinom(k, size = n, prob = p, lower=F) """
    # -- manually, not exact enough
    #sum = 0.0
    #for i in range(0, k):
        #sum+=binProb(n, p, i)
    #return sum

    # -- using scipy, not exact enough:
    # 1.0 - cdf is not  as exact as sf
    #return 1.0 - scipy.stats.distributions.binom.cdf(k-1, n, p) 

    # scipy is too complicated to compile on the cluster
    #return scipy.stats.distributions.binom.sf(k, size, prob)
    return dist.pbinom(k, size,prob)

def poissProbSum(n, p, k):
    """ poisson probability from 0 to k, k is NOT INCLUDED!"""
    sum = 0.0
    for i in range(0, k):
        sum+=poissProb(n, p, i)
    return sum

def poissProbSum_scipy(n, p, k):
    """ poisson probability from 0 to k, k is NOT INCLUDED!"""
    m = n*p
    sum=scipy.special.pdtr(k-1, m)
    return sum

def statsAddPVal(stats,flankingTargetGenes, flankingAnnotatedGenes, geneHasTargetAnnot, geneHasAnnot,noHyperg=False, geneScores=None):
    assert(False)
    # probab to find target in allannotated
    m    = len(geneHasTargetAnnot)
    N    = len(geneHasAnnot)
    # probab to find target in flankingAnnotated
    k    = len(flankingTargetGenes)
    n    = len(flankingAnnotatedGenes)

    if noHyperg:
        pVal_hgm=0
    else:
        pVal_hgm = 1.0 - util.hypergProbSum(k, N, m, n)
    stats.hypergPval    = float(pVal_hgm)
    stats.hypergParams  = {'N': N, 'm' : m, 'n' : n, 'k' : k, 'pVal' : pVal_hgm }

    if N!=0:
        p = float(m)/N
    else:
        p = 0.0

    #pVal_bp = 1.0 - util.binProbSum(n, p, k) 
    pVal_bp = util.binProbGt(k, size=n, prob=p) 

    stats.bnpPval = pVal_bp
    stats.bnpParams  = {'n': n, 'k' : k, 'pVal' : pVal_bp, 'p' : p}
    pVal_poiss = 1.0 - util.poissProbSum(n, p, k) 
    stats.pVal_poisson = pVal_poiss
    stats.poissParams  = {'lambda' : n*p, 'n': n, 'k' : k, 'pVal' : pVal_poiss, 'p' : p}

    # corrected binom. probab., using relation target CNS len / all CNS len as p
    if geneScores:
        targetScore       = sum([geneScores.get(g,0) for g in geneHasTargetAnnot]) 
        annotScore        = sum([geneScores.get(g,0) for g in geneHasAnnot]) 
        flankAnnotScore   = sum([geneScores.get(g,0) for g in flankingAnnotatedGenes]) 
        flankTargetScore  = sum([geneScores.get(g,0) for g in flankingTargetGenes]) 
        avg_All_Score = float(annotScore)/  N
        avg_Trg_Score = float(targetScore)/ m

        corrFactor = (avg_Trg_Score / (avg_All_Score+1))
        corr_p =  corrFactor * p

        #corr_pVal_bp = 1.0 - util.binProbSum(n, corr_p, k) 
        #corr_pVal_bp = 9999.0
        corr_pVal_bp = util.binProbGt(k, size=n, prob=corr_p) 
        stats.corr_bnpPval    = corr_pVal_bp
        stats.corr_bnpParams  = {'consTarget': targetScore, 'consAnnot' : annotScore, 'consFlankAnnot' : flankAnnotScore, 'consFlankTarget' : flankTargetScore, 'avgConsTarget' : avg_Trg_Score, 'avgConsAnnot' : avg_All_Score,'n': n, 'k' : k, 'pVal' : corr_pVal_bp, 'p' : corr_p, 'corrFactor' : corrFactor}

    return stats

def hitStatsWithPVal(self,predictedGenes, geneHasTargetAnnot, geneHasAnnot, noHyperg=False, geneScores=None):
    """ get stats given a set of predicted genes + calc pvalues """
    assert(False)
    stats = hitStats(geneHasAnnot, predictedGenes, geneHasTargetAnnot)
    flankingAnnotatedGenes = predictedGenes.intersection(geneHasAnnot)
    flankingTargetGenes = predictedGenes.intersection(geneHasTargetAnnot)
    statsAddPVal(stats, flankingTargetGenes, flankingAnnotatedGenes, geneHasTargetAnnot, geneHasAnnot, noHyperg, geneScores=None)
    return stats

def resolveIupac(seq):
    """ convert iupac string to regex """
    #table = { "Y" : "TCY", "R" : "GAR", "M" : "ACM", "K" : "GTK", "S" : "GCS", "W" : "ATW", "H" : "ACTHYKW", "B" : "GTCBKYS", "V" : "GCAVSR", "D" : "GATDRWK", "N" : "ACTGNYRMKWSHBVD"}
    table = { "Y" : "TC", "R" : "GA", "M" : "AC", "K" : "GT", "S" : "GC", "W" : "AT", "H" : "ACT", "B" : "GTC", "V" : "GCA", "D" : "GAT", "N" : "ACTG"}
    newseq = []
    for nucl in seq:
       if nucl in table:
	   newseq += "[%s]" % table[nucl]
       else:
	   newseq += nucl
    newstr = "".join(newseq)
    #newstr = newstr.replace("N", "[ACTGN]")
    return newstr

# copied from http://python.genedrift.org/code/dnatranslate.py
def translate_dna(sequence):
    #dictionary with the genetic code
    # modified max: accomodate CTN/CCN-codes, same as in ensembl code
    gencode = {
    'ATA':'I', 'ATC':'I', 'ATT':'I', 'ATG':'M',
    'ACA':'T', 'ACC':'T', 'ACG':'T', 'ACT':'T',
    'AAC':'N', 'AAT':'N', 'AAA':'K', 'AAG':'K',
    'AGC':'S', 'AGT':'S', 'AGA':'R', 'AGG':'R',
    'CTA':'L', 'CTC':'L', 'CTG':'L', 'CTT':'L', 
    'CTN':'L',
    'CCA':'P', 'CCC':'P', 'CCG':'P', 'CCT':'P',
    'CCN':'P',
    'CAC':'H', 'CAT':'H', 'CAA':'Q', 'CAG':'Q',
    'CGA':'R', 'CGC':'R', 'CGG':'R', 'CGT':'R',
    'CGN':'R',
    'GTA':'V', 'GTC':'V', 'GTG':'V', 'GTT':'V',
    'GTN':'V',
    'GCA':'A', 'GCC':'A', 'GCG':'A', 'GCT':'A',
    'GCN':'A',
    'GAC':'D', 'GAT':'D', 'GAA':'E', 'GAG':'E',
    'GGA':'G', 'GGC':'G', 'GGG':'G', 'GGT':'G',
    'GGN':'G',
    'TCA':'S', 'TCC':'S', 'TCG':'S', 'TCT':'S',
    'TCN':'S',
    'TTC':'F', 'TTT':'F', 'TTA':'L', 'TTG':'L',
    'TAC':'Y', 'TAT':'Y', 'TAA':'_', 'TAG':'_',
    'TGC':'C', 'TGT':'C', 'TGA':'_', 'TGG':'W',
    }
    
    proteinseq = ''
    #loop to read DNA sequence in codons, 3 nucleotides at a time
    for n in range(0,len(sequence),3):
        #checking to see if the dictionary has the key
        if gencode.has_key(sequence[n:n+3]) == True:
            proteinseq += gencode[sequence[n:n+3]]
        else:
            proteinseq += "X" # modif max: to make it the same as ensembl
    #return protein sequence
    return proteinseq

def stripUtrs(cdnaSeq, pepSeq):
    """ using peptide sequence, remove utrs from cdna sequence: translate in all 3 frames, search peptide, remove flanking parts """
    pepSeqFrames = []
    pepSeqFrames.append(translate_dna(cdnaSeq))
    pepSeqFrames.append(translate_dna(cdnaSeq[1:]))
    pepSeqFrames.append(translate_dna(cdnaSeq[2:]))
    uPepSeq = pepSeq.replace("-","")
    pepRe = re.compile(uPepSeq)
    frame=0
    for trans in pepSeqFrames:
        #print "frame=",frame
        #print "cdna     :", cdnaSeq
        #print "trans    :", "--".join(trans)
        #print "orig     :", "--".join(uPepSeq)
        #print "found    :", trans.find(uPepSeq)
        match = pepRe.search(trans)
        if match!=None:
            start = match.start()
            end   = match.end()
            #print start,end
            return cdnaSeq[start*3+frame:end*3+frame]
        frame+=1

def findSubdirFiles(baseDir, extension):
    """ Generator: traverse a baseDir and all subdirectories to find all files with a certain extension, extension is dot plus the extension, like ".xml"  """
    #result = []
    for root, dirs, files in os.walk(baseDir):
        for f in files:
            if extension==None or os.path.splitext(f)[1]==extension:
                path = os.path.join(root, f)
                yield path

def baseNFill(num, base, numerals, length):
    """ like baseN, but will fill up with the first symbol (=0) up to length """
    text = baseN(num, base, numerals)
    while len(text) < length:
        text = numerals[0]+text
    return text

def baseN(num, base=49, numerals="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    """
    Convert any int to base/radix 2-36 string. Special numerals can be used
    to convert to any base or radix you need. This function is essentially
    an inverse int(s, base).

    For example:
    >>> baseN(-13, 4)
    '-31'
    >>> baseN(91321, 2)
    '10110010010111001'
    >>> baseN(791321, 36)
    'gyl5'
    >>> baseN(91321, 2, 'ab')
    'babbaabaababbbaab'
    """
    if num == 0:
        return numerals[0]

    if num < 0:
        return '-' + baseN((-1) * num, base, numerals)

    if not 2 <= base <= len(numerals):
        raise ValueError('Base must be between 2-%d' % len(numerals))

    left_digits = num // base
    if left_digits == 0:
        return numerals[num % base]
    else:
        return baseN(left_digits, base, numerals) + numerals[num % base]

def remove_accents(str):
    """ remove accents from unicode string and return as ascii, replace with non-accented similar ascii characters """
    nkfd_form = unicodedata.normalize('NFKD', unicode(str))
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])

def removeAccents(unicodeString):
    """ remove accents from unicode string and return as ascii, replace with non-accented similar ascii characters """
    nkfd_form = unicodedata.normalize('NFKD', unicodeString) # replace accents
    cleanStr = u"".join([c for c in nkfd_form if not unicodedata.combining(c)]) # remove diacritics
    cleanStr = u"".join([c for c in cleanStr if ord(c) < 128]) # remove diacritics
    return cleanStr.decode("ascii")

def makeDirs(dir):
    """ if it does not exist, create it. expand unix spec chars """
    dir = os.path.expanduser(dir)
    if not os.path.isdir(dir):
        #path=shell.SHGetFolderPath(0, shellcon.CSIDL_PERSONAL, None, 0)
        log("creating %s" % dir)
        os.makedirs(dir) 
    return dir 

def sortTable(inFname, outFname, fieldIdx):
    """ use unix sort to sort a tab sep table by a given field. FieldIdx is 1-based. """
    fieldIdx = int(fieldIdx)
    logging.info("Sorting %s to %s on field %d" % (inFname, outFname, fieldIdx))
    cmd = "sort -t'\t' --key=%d %s -o %s" % (fieldIdx, inFname, outFname)
    execCmdLine(cmd)
    
