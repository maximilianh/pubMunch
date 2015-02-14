# run untrusted python code on parasol
import glob, sys, subprocess, tempfile, os, random, shutil, hashlib
from os.path import join, basename, isdir
from os import mkdir, umask

import jobQueue, sqlite3

CLUSTER = "encodek"
BASE = "/hive/data/inside/pubs/webRuns"
WEBRUNDIR = BASE+"/sandbox/runs"
TEXTDIRS = [BASE+"/sandbox/text/pmc"]
# to translate from outside to inside path
CONV_REAL = BASE+"/sandbox/"
CONV_SANDBOX = "/tmp/"

JOBSCRIPT = BASE+"/sandbox/runScript.sh"
JOBQUEUE = BASE+"/jobs.sqlite"

JOBCONCATDIR = "/cluster/home/max/public_html/mining/jobOutput/"
# RELATIVE FROM HTDOCS dir!
JOBCONCATURL = "../jobOutput"

def createTempDir(webRunDir, codeStr):
    jobId = hashlib.sha1(codeStr).hexdigest()
    jobDir = join(webRunDir, jobId)
    if not isdir(jobDir):
        mkdir(jobDir)
    return jobDir, jobId

def writeCode(code, outDir):
    fname = join (outDir, "webCode.py")
    f = open(fname, "w")
    f.write(code)
    f.close()
    sandboxPath = fname.replace(CONV_REAL, CONV_SANDBOX)
    #return fname
    return sandboxPath

def findTextFnames(textDirs, outDir):
    " return list of pairs (inFname, outFname) given a list of directory with text files "
    inOutFnames = []
    for textDir in textDirs:
        textBase = basename(textDir)
        inFnames = glob.glob(join(textDir, "*.articles"))
        for inFname in inFnames:
            inBase = basename(inFname).split(".")[0]
            outFname = join(outDir, textBase+"_"+inBase+".tab")
            inFnameSbox = inFname.replace(CONV_REAL, CONV_SANDBOX)
            inOutFnames.append( (inFnameSbox, outFname) )
    return inOutFnames

def createJobList(batchDir, codeFname, inOutFnames, forParasol=True):
    jobListName = join(batchDir, "jobList")
    jobListFh = open(jobListName, "w")
    jobs = []
    for inFname, outFname in inOutFnames:
        if forParasol:
            outFnameSpec = "{check out exists %s}" % outFname
        else:
            outFnameSpec = outFname
        cmd = "%s %s %s %s" % \
                (JOBSCRIPT, codeFname, inFname, outFnameSpec)
        jobListFh.write(cmd+"\n")
        jobs.append(cmd)

    jobListFh.close()
    return jobListName, jobs

def runParaMake(headnode, batchDir, jobListFname):
    cmd = ["ssh", headnode, "cd %s; para make %s" % (batchDir, jobListFname)]
    subprocess.check_call(cmd)

def runLocally(jobListFname):
    #cmd = "nohup sh %s" % jobListFname
    #cmd = "sh %s" % jobListFname
    #os.system(cmd)
    pass

def runCode(codeStr):
    os.umask(000) # tempdirs need to be accessible to workers, not only apache
    tmpDir, batchId = createTempDir(WEBRUNDIR, codeStr)
    codeFname = writeCode(codeStr, tmpDir)
    batchDir = tmpDir+"/parasol"
    outDir   = tmpDir+"/out"
    if not isdir(batchDir):
        mkdir(batchDir)
    if not isdir(outDir):
        mkdir(outDir)

    inOutFnames = findTextFnames(TEXTDIRS, outDir)
    jobListFname, jobs = createJobList(batchDir, codeFname, inOutFnames, forParasol=False)
    jq = jobQueue.JobQueue(JOBQUEUE)
    jobs = jobs[0:3]
    try:
        jq.appendJobs(jobs, batchId)
    # if jobId already exists, cannot add to queue. Unlikely. (SHA1)
    except sqlite3.IntegrityError:
        pass 
        
    #runParaMake(CLUSTER, batchDir, jobListFname)
    #runLocally(jobListFname)
    return batchId

def getStatus(batchId):
    #outDirs = set(os.listdir(WEBRUNDIR))
    #if jobId in outDirs:
        #status = "completed"
        #randomLines = open(join(WEBRUNDIR, jobId, "randomLines.tab"), "r").read()
        #datasetPath = join(WEBRUNDIR, jobId, "result.tab.gz")
    jq = jobQueue.JobQueue(JOBQUEUE)
    statusMsg = jq.getStatus(batchId)
    sampleLines, allUrl = None, None
    if statusMsg==None:
        statusMsg =  "Job ID not found" 
    else:
        if statusMsg.startswith("all complete"):
            sampleLines = open(join(JOBCONCATDIR,batchId+".randomLines.tab")).read().splitlines()
            allUrl      = join(JOBCONCATURL,batchId+".tab.gz")
    return statusMsg, sampleLines, allUrl
