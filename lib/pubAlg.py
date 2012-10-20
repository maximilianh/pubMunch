# some algorithms to run over text files

# includes functions to load algorithms from external python files,
# run them on pubStores, do map/reduce like algorithms on cluster, etc

# module will call itself on the compute nodes if run on a cluster (->findFileSubmitJobs)

import logging, sys, os, shutil, glob, cPickle, optparse, copy, types, string, pipes, gzip, doctest, marshal

from os.path import *
from maxCommon import *

import protDetect, unifyAuthors, pubGeneric, maxRun, pubConf, pubStore, pubAlg, maxCommon

# extension of map output files
MAPREDUCEEXT = ".pickle.gz"

def loadClass(aMod, className, quiet=False):
    " try to find class in a module and return it if found, otherwise None "
    logging.debug("trying to load class %s" % className)
    if className==None:
        return None
    if not hasattr(aMod, className):
        if not quiet:
            logging.debug("Could not find class %s " % (className))
        return None
    else:
        ClassObj = getattr(aMod, className)
        alg   = ClassObj()
        logging.debug("Instantiated class %s in module" % className)
        return alg

def loadPythonObject(moduleFilename, className, defClass=None):
    """ get function or object from dynamically loaded .py file """
    # must add path to system search path first
    if not os.path.isfile(moduleFilename):
        moduleFilename = join(pubConf.scriptDir, moduleFilename)

    if not os.path.isfile(moduleFilename):
        logging.error("Could not find %s" % moduleFilename)
        sys.exit(1)
    modulePath, moduleName = os.path.split(moduleFilename)
    moduleName = moduleName.replace(".py","")
    logging.debug("Loading python code from %s (class %s)" % (moduleFilename, className))
    sys.path.append(modulePath)

    # load algMod as a module, copied from 
    # http://code.activestate.com/recipes/223972-import-package-modules-at-runtime/
    try:
        aMod = sys.modules[moduleName]
        if not isinstance(aMod, types.ModuleType):
            raise KeyError
    except KeyError:
        # The last [''] is very important!
        aMod = __import__(moduleName, globals(), locals(), [''])
        sys.modules[moduleName] = aMod

    # first try className, then default class, then module itself
    alg = loadClass(aMod, className, quiet=True)
    if alg==None:
        alg = loadClass(aMod, defClass)
    if alg==None:
        return aMod
    else:
        return alg

def getAlg(algName, defClass=None):
    """ given a name, returns an alg object

    name can be name of a python module 
    or moduleName:className

    object or module needs to support the operation annotate(string) and the 
    variable "headers"

    defaultClass can be "Annotate" or "Map"
    """
    logging.debug("Creating algorithm object for %s " % (algName))
    if algName=="upcaseCount":
        alg = protDetect.UpcaseCounter()
    elif algName=="ProteinDetect":
        alg = protDetect.ProteinDetect()
    elif algName=="unifyAuthors":
        alg = unifyAuthors.UnifyAuthors()
    elif algName=="getFileDesc":
        alg = unifyAuthors.GetFileDesc()
    else:
        if ":" in algName:
            filename, className = algName.split(":")
        else:
            filename, className = algName, None
        if filename.endswith(".pyc"):
            filename = filename.replace(".pyc", "")
        if not filename.endswith(".py"):
            filename = filename+".py"
        alg = loadPythonObject(filename, className, defClass=defClass)
    #else:
        #logging.error("algorithm name %s is unknown" % algName)
        #sys.exit(1)
    return alg

def writeParamDict(paramDict, paramDictName):
    " pickle parameter to current dir "
    logging.debug("Writing parameters to %s" % paramDictName)
    outFh = open(paramDictName, "w")
    cPickle.dump(paramDict, outFh)
    return paramDictName

def findFiles(inDir):
    """ return all basenames for .gz files in inDir. 
    inDir can be a list of datasetnames, a file or a directory with datasetnames """
    if type(inDir)==types.ListType:
        datasets = inDir
        zipNames = []
        for dataset in datasets:
            dataDir = resolveTextDir(dataset)
            zipNames.extend(glob.glob(join(dataDir, "*.gz")))
    elif isfile(inDir):
        zipNames = [inDir]
    elif not isdir(inDir):
        inDir = resolveTextDir(inDir)
        zipNames = glob.glob(join(inDir, "*.gz"))
    elif isdir(inDir):
        zipNames = glob.glob(join(inDir, "*.gz"))
    else:
        assert(False)

    if len(zipNames)==0:
        raise Exception("Could not find any gz files in %s"% inDir)

    return zipNames

def findArticleBasenames(inDir, updateIds=None):
    """ given a fulltext directory, return all basenames of *.{article,files}.gz files 
        Basename means the part before the first "."
        Optionally filters on updateId
    
    """
    zipNames = findFiles(inDir)
    logging.debug("Found zipfiles: %d files" % len(zipNames))
    baseNames = set([join(dirname(fn), basename(fn).split(".")[0]) for fn in zipNames])
    logging.debug("Found filenames: %d files" % len(baseNames))
    if updateIds!=None:
        filteredNames = []
        for updateId in updateIds:
            updateId = str(updateId)
            logging.debug("Removing all filenames that don't start with updateId %s" % updateId)
            filteredNames.extend([fn for fn in baseNames if basename(fn).startswith("%s_" % updateId)])
        baseNames = filteredNames
    logging.debug("Found %s basenames in %s: " % (len(baseNames), inDir))
    return baseNames

def findFilesSubmitJobs(algNames, algMethod, inDir, outDirs, outExt, paramDict, runNow=False, cleanUp=False, updateIds=None, batchDir=".", runner=None):
    """ find data zip files and submit one map job per zip file
        Jobs call pubAlg.pyc and then run the algMethod-method of algName

        cleanUp: remove temporary files
        runNow: wil block until jobs are completed, then return

        If a list of updateIds is specified, run only on files with one of these updateIds
        Returns the list of baseNames, e.g. 0_00000,0_00001, etc that it ran on
    """
    assert(algMethod in ["map", "annotate"]) 
    assert(algNames.count(",")==outDirs.count(",")) # need several output dirs if multiple algs specified

    baseNames = findArticleBasenames(inDir, updateIds)
    algNames = algNames.split(",")
    outDirs = outDirs.split(",")

    if runner==None:
        runner = maxRun.Runner(batchDir=batchDir)

    algCount = 0
    for algName, outDir in zip(algNames, outDirs):
        algShortName = basename(algName).split(".")[0]
        outDir = abspath(outDir)
        paramFname = join(outDir, algShortName+".mapReduceParam.conf")
        # if multiple algs specified: try to make annotIds non-overlapping
        paramKey = "startAnnotId."+algShortName
        if paramKey not in paramDict and len(algNames)>1:
            paramDict[paramKey] = str(algCount*(10**pubConf.ANNOTDIGITS/len(algNames)))

        writeParamDict(paramDict, paramFname)
        for inFile in baseNames:
            inBase = splitext(basename(inFile))[0]
            outFullname = join(outDir, inBase)+outExt
            mustNotExist(outFullname)
            command = "%s %s %s %s %s {check out exists %s} %s" % \
                (sys.executable, __file__ , algName, algMethod, inFile, outFullname, paramFname)
            runner.submit(command)
        algCount += 1

    runner.finish(wait=runNow, cleanUp=cleanUp)
    if cleanUp:
        os.remove(paramFname)
    return baseNames
    
#def getDataIterator(alg, reader):
#    """ depending on the field "runOn" return the right
#     type of iterator of the reader 
#    """
#    if "runOn" in dir(alg):
#        if alg.runOn=="articles":
#            logging.debug("algorithm asked for only articles")
#            iterator = reader.iterArticles()
#        elif alg.runOn=="files":
#            logging.debug("algorithm asked for only files")
#            iterator = reader.iterFiles()
#        elif alg.runOn=="all" or alg.runOn=="both":
#            logging.debug("algorithm asked for files and articles")
#            iterator = reader.iterFileArticles()
#        else:
#            raise Exception("Illegal value for 'runOn' attribute in algorithm")
#    else:
#        logging.debug("algorithm did not specify any target data, selecting files & articles")
#        iterator = reader.iterFileArticles()
#    return iterator

def findBestSnippet(text, start, end, minPos, maxPos, isLeft=False):
    " get end or start pos of best snippet for (start, end) in range (minPos, maxPos)"
    textLen = len(text)
    #print "isLeft", isLeft, "corr", minPos, maxPos
        
    # make sure that (min,max) stays within string boundaries
    # and does not go into (start,end)
    if isLeft:
       minPos = max(0, minPos)
       maxPos = max(maxPos, 0)
       dotPos = text.rfind(". ", minPos, maxPos)
    else:
       minPos = max(0, minPos)
       maxPos = min(maxPos, textLen)
       dotPos = text.find(". ", minPos, maxPos)

    if dotPos==-1:
        if isLeft:
            dotPos = maxPos
            if dotPos==start:
                dotPos=minPos
        else:
            dotPos = minPos
            if dotPos==end:
                dotPos=maxPos
    elif isLeft:
        dotPos+=2
    else:
        dotPos+=1

    return dotPos

def getSnippet(text, start, end, minContext=0, maxContext=250):
    """ return contextLen characters around start:end from text string 
    >>> textWithDot = 'A sentence. Another sentence. XXX. And yes a sentence. Oh my. Oh my.'
    >>> Xpos = textWithDot.find("X")
    >>> getSnippet(textWithDot, Xpos, Xpos+3, minContext=5, maxContext=30)
    'Another sentence. <<<XXX>>>. And yes a sentence.'
    >>> textWithDot = 'cex XXX And'
    >>> Xpos = textWithDot.find("X")
    >>> getSnippet(textWithDot, Xpos, Xpos+3, minContext=5, maxContext=30)
    'cex <<<XXX>>> And'
    >>> textWithDot = 'XXX'
    >>> getSnippet(textWithDot, 0, 4, minContext=5, maxContext=30)
    '<<<XXX>>>'
    >>> textWithDot = 'A sentence without a dot yes XXX well there is no dot anywhere here'
    >>> Xpos = textWithDot.find("X")
    >>> getSnippet(textWithDot, Xpos, Xpos+3, minContext=5, maxContext=30)
    ' yes <<<XXX>>> well'
    >>> textWithDot = 'Hihi. bobo. X A sentence that starts with a dot.'
    >>> Xpos = textWithDot.find("X")
    >>> getSnippet(textWithDot, Xpos, Xpos+1, minContext=0, maxContext=50)
    """
    start = int(start)
    end = int(end)
    rightDotPos = findBestSnippet(text, start, end, end+minContext, end+maxContext, isLeft=False)
    leftDotPos = findBestSnippet(text, start, end, start-maxContext, start-minContext, isLeft=True)

    leftSnip = text[leftDotPos:start]
    mainSnip = text[start:end]
    rightSnip = text[end:rightDotPos]
    snippet = leftSnip+"<<<"+mainSnip+">>>"+rightSnip
    snippet = snippet.replace("\n", " ")
    snippet = snippet.replace("\t", " ")
    return snippet

def writeAnnotations(alg, articleData, fileData, outFh, annotIdAdd, doSectioning):
    """ use alg to annotate fileData, write to outFh, adding annotIdAdd to all annotations 
    return next free annotation id.
    """
    annotDigits = int(pubConf.ANNOTDIGITS)
    fileDigits = int(pubConf.FILEDIGITS)
    annotIdStart = (int(fileData.fileId) * (10**annotDigits)) + annotIdAdd
    logging.debug("fileId %s, annotIdStart %d" % (fileData.fileId, annotIdStart))

    text = fileData.content.replace("\a", "\n")

    allTextSections = {"unknown": (0, len(text))}
    if doSectioning:
        sections = pubGeneric.sectionRanges(text)
        if sections==None:
            sections = allTextSections
    else:
        sections = allTextSections

    annotCount = 0
    for section, sectionRange in sections.iteritems():
        secStart, secEnd = sectionRange
        if section!="unknown":
            logging.debug("Annotating section %s, from %d to %d" % (section, secStart, secEnd))
        secText = text[secStart:secEnd]
        fileData = fileData._replace(content=secText)
        annots = alg.annotateFile(articleData, fileData)
        if annots==None:
            logging.debug("No annotations received")
            continue

        for row in annots:
            # prefix with fileId, extId
            logging.debug("received annotation row: %s" %  row)
            fields = ["%018d" % (int(annotIdStart)+annotCount)]
            if articleData!=None:
                extId = articleData.externalId
            else:
                extId = "0"
            fields.append(extId)
            # add other fields
            fields.extend(row)

            # lift start and end if sectioning
            start, end = row[0:2]
            snippet = getSnippet(secText, start, end)
            start = secStart+int(start)
            end = secStart+int(end)

            # postfix with snippet
            logging.debug("Got row: %s" % str(row))
            if doSectioning:
                fields.append(section)
            fields.append(snippet)
            #fields = [unicode(x).encode("utf8") for x in fields]
            fields = [unicode(x) for x in fields]
                
            line = "\t".join(fields)
            outFh.write(line+"\n")
            annotCount+=1
            assert(annotCount<10**annotDigits)
    return annotCount

def writeHeaders(alg, outFh, doSectioning):
    " write headers from algorithm to outFh "
    if "headers" not in dir(alg) and not "headers" in alg.__dict__:
        logging.error("headers variable not found.")
        logging.error("You need to define a variable 'headers' in your python file or class")
        sys.exit(1)

    headers = copy.copy(alg.headers)
    headers.insert(0, "annotId")
    headers.insert(1, "externalId")
    if doSectioning:
        headers.append("section")
    headers.append("snippet")
    logging.debug("Writing headers %s to %s" % (headers, outFh.name))
    outFh.write("\t".join(headers)+"\n")

def getAlgName(alg):
    " return name of algorithm: either name of module or name of class "
    algName = alg.__class__.__name__
    if algName=="module":
        algName = alg.__name__
    return algName

def getAnnotId(alg, paramDict):
    """ return annotId configured by paramDict with parameter startAnnotId.<algName>, 
    remove parameter from paramDict
    """
    algName = getAlgName(alg)
    paramName = "startAnnotId."+algName

    annotIdAdd = int(paramDict.get(paramName, 0))
    if paramDict.get(paramName, None):
        del paramDict[paramName]
    return annotIdAdd

def makeLocalTempFile():
    " create tmp file on local harddisk "
    fd, tmpOutFname = tempfile.mkstemp(dir=pubConf.getTempDir(), prefix="pubRun", suffix=".tab")
    os.close(fd)
    logging.debug("local temporary file is %s" % tmpOutFname)
    return tmpOutFname

def moveTempToFinal(tmpOutFname, outFname):
    " copy from temp to final out dest fname "
    logging.debug("Copying %s to %s" % (tmpOutFname, outFname))
    outDir = dirname(outFname)
    if outDir!="" and not isdir(outDir):
        os.makedirs(outDir)
    shutil.copy(tmpOutFname, outFname)
    os.remove(tmpOutFname)

def attributeTrue(obj, attrName):
    " returns true if obj has attribute and it is true "
    if attrName in dir(obj):
        if obj.__dict__[attrName]==True:
            return True
    return False

def runAnnotate(reader, alg, paramDict, outName):
    """ annotate all articles in reader
    """
    tmpOutFname = makeLocalTempFile()

    outFh = pubStore.utf8GzWriter(tmpOutFname)

    doSectioning = attributeTrue(alg, "sectioning")
    logging.debug("Sectioning activated: %s" % doSectioning)

    if "startup" in dir(alg):
        logging.debug("Running startup")
        alg.startup(paramDict)

    writeHeaders(alg, outFh, doSectioning)

    annotIdAdd = getAnnotId(alg, paramDict)

    onlyMeta = attributeTrue(alg, "onlyMeta")
    bestMain = attributeTrue(alg, "bestMain")

    for articleData, fileDataList in reader.iterArticlesFileList(onlyMeta, bestMain):
        logging.debug("Annotating article %s with %d files, %s" % \
            (articleData.articleId, len(fileDataList), [x.fileId for x in fileDataList]))
        for fileData in fileDataList:
            writeAnnotations(alg, articleData, fileData, outFh, annotIdAdd, doSectioning)

    outFh.close()
    moveTempToFinal(tmpOutFname, outName)

def runMap(reader, alg, paramDict, outFname):
    """ run map part of alg over all files that reader has.
        serialize results ('pickle') to outFname 
        
        input can be a reader or a directory
        alg can be a string or an alg object 
    """
    tmpOutFname = makeLocalTempFile()

    results = {}
    if "startup" in dir(alg):
        alg.startup(paramDict, results)

    # run data through algorithm
    onlyMeta = attributeTrue(alg, "onlyMeta")
    bestMain = attributeTrue(alg, "bestMain")

    for articleData, fileDataList in reader.iterArticlesFileList(onlyMeta, bestMain):
        logging.debug("Running on article id %s" % articleData.articleId)
        for fileData in fileDataList:
            logging.debug("Running on file id %s" % fileData.fileId)
            text = fileData.content

        alg.map(articleData, fileData, text, results)

    if "end" in dir(alg):
        results = alg.end(results)

    outFh = gzip.open(tmpOutFname, "wb")
    binData = marshal.dumps(results)
    outFh.write(binData)
    outFh.close()
    del binData

    moveTempToFinal(tmpOutFname, outFname)

def runReduce(algName, paramDict, path, outFilename, quiet=False):
    """ parse pickled dicts from path, run through reduce function of alg and 
    write output to one file """

    if isfile(outFilename):
        logging.info("deleting existing file %s" % outFilename)
        os.remove(outFilename)

    if isinstance(algName, basestring):
        alg = getAlg(algName, defClass="Map")
    else:
        alg = algName

    if "startup" in dir(alg):
        alg.startup(paramDict, {})

    if isfile(path):
        logging.debug("Filename specified, running only on a single file (debugging)")
        infiles = [(dirname(path), path)]
    else:
        infiles = pubGeneric.findFiles(path, [MAPREDUCEEXT])
    
    if len(infiles)==0:
        logging.error("Could not find any %s files in %s" % (MAPREDUCEEXT, path))
        sys.exit(1)

    # read pickle files into data dict
    data = {}
    fileCount = 0
    logging.info("Reading map output")
    meter = maxCommon.ProgressMeter(len(infiles), quiet=quiet, stepCount=100)
    for relDir, fileName in infiles:
        binData = gzip.open(fileName, "rb").read()
        nodeData = marshal.loads(binData)
        del binData
        for key, values in nodeData.iteritems():
            if not hasattr(values, "__iter__"):
                values = [values]
            data.setdefault(key, []).extend(values)
        fileCount+=1
        logging.debug("Reading "+fileName)
        meter.taskCompleted()

    if "reduceStartup" in dir(alg):
        logging.info("Running reduceStartup")
        alg.reduceStartup(data, paramDict)

    logging.info("Writing to %s" % outFilename)
    if outFilename=="stdout":
        ofh = sys.stdout
    else:
        ofh = open(outFilename, "w")
    ofh.write("\t".join(alg.headers))
    ofh.write("\n")

    logging.info("Running data through reducer")
    meter = maxCommon.ProgressMeter(len(data))
    for key, valList in data.iteritems():
        tupleIterator = alg.reduce(key, valList)
        for tuple in tupleIterator:
            if tuple==None:
                continue
            if type(tuple)==types.StringType: # make sure that returned value is a list
                tuple = [tuple]
            if type(tuple)==types.IntType: # make sure that it's a string
                tuple = [str(tuple)]
            tuple = [unicode(x).encode("utf8") for x in tuple] # convert to utf8
            ofh.write("\t".join(tuple))
            ofh.write("\n")
        meter.taskCompleted()
    ofh.close()

def annotate(algNames, textDir, paramDict, outDirs, cleanUp=False, runNow=False, updateIds=None, batchDir=".", runner=None):
    """ 
    submit jobs to batch system to run algorithm over text in textDir, write
    annotations to outDir 

    algNames can be a comma-sep list of names
    outDirs can be a comma-sep list of directories
    """
    for algName in algNames.split(","):
        logging.debug("Testing algorithm %s startup" % algName)
        alg = getAlg(algName, defClass="Annotate") # just to check if algName is valid
        if "startup" in dir(alg):
            alg.startup(paramDict) # to check if startup works

    baseNames = findFilesSubmitJobs(algNames, "annotate", textDir, outDirs, \
        ".tab.gz", paramDict, runNow=runNow, cleanUp=cleanUp, updateIds=updateIds, batchDir=batchDir, runner=runner)
    return baseNames

def mapReduceTestRun(textDir, alg, paramDict, tmpDir, updateIds=None, skipMap=False):
    " do a map reduce run only on one random file, no cluster submission, for testing "
    if updateIds!=None:
        updateId = updateIds[0]
    else:
        updateId = None
    baseNames = findArticleBasenames(textDir, updateId)
    firstBasename = baseNames.pop()
    oneInputFile = firstBasename+".articles.gz"
    if not isfile(oneInputFile):
        oneInputFile = firstBasename+".files.gz"
    logging.info("Testing algorithm on file %s" % oneInputFile)
    reader = pubStore.PubReaderFile(oneInputFile)
    tmpAlgOut = join(tmpDir, "pubMapReduceMap.tmp")
    tmpRedOut = join(tmpDir, "pubMapReduceReduce.tmp")
    if skipMap==False or not isfile(tmpAlgOut):
        runMap(reader, alg, paramDict, tmpAlgOut)
    runReduce(alg, paramDict, tmpAlgOut, tmpRedOut, quiet=True)
    ifh = open(tmpRedOut)
    logging.info("Example reducer output")
    for i in range(0, 10):
        line = ifh.readline()
        line = line.strip()
        logging.info(line)
    #os.remove(tmpAlgOut)
    os.remove(tmpRedOut)

def resolveTextDir(textDir):
    " make sure textDir exists and try the default locations to resolve it to full path "
    logging.debug("Resolveing %s to directory" % textDir)
    if not isdir(textDir) and not isfile(textDir):
        textDir = join(pubConf.textBaseDir, textDir)
        if not isdir(textDir):
            logging.error("Cannot find dir %s" % textDir)
            sys.exit(1)
    return textDir

def mapReduce(algName, textDir, paramDict, outFilename, skipMap=False, cleanUp=False, tmpDir=None, deleteDir=True, updateIds=None, runTest=True, batchDir=".", headNode=None, runner=None):
    """ 
    submit jobs to batch system to:
    create tempDir, map textDir into this directory with alg,
    then reduce from tmpDir to outFilename 

    will test the algorithm on a random input file first
    if updateIds is set, will only run on files like <updateId>_*, otherwise on all files
    """

    alg = getAlg(algName, defClass="Map") # just to check if algName is valid

    if tmpDir==None:
        tmpDir = join(pubConf.mapReduceTmpDir, os.path.basename(algName).split(".")[0])
    if skipMap:
        assert(isdir(tmpDir))
    else:
        if isdir(tmpDir):
            logging.info("Deleting map/reduce temp directory %s" % tmpDir)
            shutil.rmtree(tmpDir)
        logging.info("Creating directory %s" % tmpDir)
        os.makedirs(tmpDir)

    # before we let this loose on the cluster, make sure that it actually works
    if runTest:
        mapReduceTestRun(textDir, alg, paramDict, tmpDir, updateIds=updateIds, skipMap=skipMap)

    logging.info("Now submitting to cluster/running on all files")
    if not skipMap:
        findFilesSubmitJobs(algName, "map", textDir, tmpDir, MAPREDUCEEXT, paramDict,\
            runNow=True, cleanUp=cleanUp, updateIds=updateIds, batchDir=batchDir, runner=runner)
    runReduce(algName, paramDict, tmpDir, outFilename)

    if deleteDir and not skipMap:
        logging.info("Deleting directory %s" % tmpDir)
        shutil.rmtree(tmpDir)

if __name__ == '__main__':
    parser = optparse.OptionParser("""this module is calling itself. 
    syntax: pubAlg.py <algName> map|reduce <inFile> <outFile> <paramPickleFile>
    """)
    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
    (options, args) = parser.parse_args()
    pubGeneric.setupLogging(__file__, options)

    if len(args)==0:
        doctest.testmod()
        sys.exit(0)

    algName, algMethod, inName, outName, paramFile = args
    paramDict = cPickle.load(open(paramFile))

    #mustExist(inName)

    alg = pubAlg.getAlg(algName, defClass=string.capitalize(algMethod))
    #alg = pubAlg.getAlg(algName, defClass=algMethod)
    reader = pubStore.PubReaderFile(inName)

    if algMethod=="map":
        runMap(reader, alg, paramDict, outName)
    elif algMethod=="annotate":
        runAnnotate(reader, alg, paramDict, outName)

    reader.close()
    
