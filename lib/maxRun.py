# module to run commands, either locally, or on parasol or sge cluster
# will autodetect cluster system

# can also run python functions instead of commands by calling itself on the cluster
# system and then calling the function (see main() and submitPythonFunc)

import os, sys, logging, shutil, types, optparse

class Runner:
    """
    a class that runs commands or python methods on cluster (sge/parasol/local) 
    """

    def __init__(self, clusterType="auto", headNode=None, queue=None, dryRun=False, \
        logDir=None, delayTime=None, maxPush=700000, maxJob=None, batchDir=".", runNow=False, maxRam=None):
        """ create joblist on parasol, do nothing on SGE 
            clusterType can be "local", "sge" or "parasol"

            if headNode is set, will ssh onto headNode to submit batch and cd into batchDir or cwd

            if runNow is set, will block until job is finished (doesn't work on SGE)

            delayTime, maxPush, maxJob are only used for parasol
            queue is only used for SGE
        """
        self.jobListFh = None
        self.clusterType = clusterType
        self.headNode = headNode
        self.logDir = logDir
        self.dryRun = dryRun
        self.queue = queue
        self.delayTime = delayTime
        self.maxPush = maxPush
        self.maxJob = maxJob
        self.batchDir = batchDir
        self.runNow = runNow 
        self.maxRam = maxRam
        self.jobCount = 0
        if not os.path.isdir(batchDir):
            logging.debug("creating dir %s" % batchDir)
            os.makedirs(batchDir)

        # auto-detect cluster type
        if self.clusterType=="auto":
            ret = os.system("para 2> /dev/null")
            if ret==65280:
                self.clusterType="parasol"
            else:
                sge = os.environ.get("SGE_ROOT", None)
                if sge!=None:
                    self.clusterType="sge"
                else:
                    self.clusterType="local"
            logging.info("Cluster type autodetect: %s" % self.clusterType)

        if self.clusterType=="parasol":
            self.jobListFname = os.path.join(self.batchDir, "jobList")
            self.jobListFh = open(self.jobListFname, "w")
            logging.info("Created jobList file in %s" % self.jobListFname)
        elif self.clusterType in ["sge","local"]:
            pass
        else:
            logging.error("Illegal cluster type")
            exit(1)

    def _exec(self, cmdLine, stopOnError=False):
        """ internal function to run command via os.system, even if markd doesn't liek it """
        if not self.dryRun:
            logging.debug("Running: %s" % cmdLine)
            ret = os.system(cmdLine)
        else:
            logging.info("Dry-run, not executing: %s" % cmdLine)
            ret = 0
            print cmdLine

        if ret!=0:
            logging.error("Error %d when executing command: %s" % (ret, cmdLine))
            if stopOnError:
                logging.info("Runner told to stop on errors")
                sys.exit(1)
        if ret==2:
            logging.error("Seems that you pressed ctrl-c, quitting")
            sys.exit(1)

    def submit(self, command, jobName=None):
        """ submit command on sge, add command to joblist for parasol 

        removes the special parasol tags {check out line, {check in line etc for SGE/local
        
        """

        self.jobCount += 1

        if type(command)==types.ListType:
            command = " ".join(command)

        if self.clusterType in ["sge", "local"]:
            # remove parasol tags
            removeTags = [ "}",
                "{check in line",
                "{check out line+",
                "{check out line",
                "{check out exists+",
                "{check out exists",
                "{check in exists+",
                "{check in exists"
                ]
            for tag in removeTags:
                command = command.replace(tag, "")

        if self.clusterType=="sge":
            options = ""
            if self.logDir!=None:
                logExpr = "$JOB_ID-$JOB_NAME.out" 
                logFullPath = os.path.join(self.logDir,logExpr)
                options += "-o '"+logFullPath+ "' "

            if self.queue:
                options += "-q %s " % self.queue

            if jobName:
                options += "-N '%s'" % jobName

            cmdLine = "qsub -V -j y -b y -cwd %s %s" % (options, command)
            self._exec(cmdLine)

        elif self.clusterType=="local":
            logging.info("Running command: %s" % command)
            self._exec(command, stopOnError=True)

        elif self.clusterType=="parasol":
            self.jobListFh.write(command)
            self.jobListFh.write("\n")

    def submitPythonFunc(self, moduleName, funcName, params, jobName=None):
        """ this will call our wrapper to run a python function instead of 
        an executable program. The list of parameters should not contain any special characters (=only filenames)
        """
        command = " ".join([sys.executable, __file__ , moduleName, funcName, " ".join(params)])
        # will to resolve to something like:
        # /usr/bin/python <dir>/maxRun.py myMod myFunc hallo test
        self.submit(command)

    def finish(self, wait=False, cleanUp=False):
        """ submit joblist to parasol, do nothing on SGE """
        if self.jobCount==0:
            logging.warn("No jobs submitted, not running anything")
            return

        #assert(self.jobCount > 0)
        if self.clusterType=="parasol":
            self.jobListFh.close()
            logging.info("Running batch from file '%s'" % self.jobListFh.name)
            if wait or self.runNow:
                paraCmd = "make"
            else:
                paraCmd = "create"

            cmd = "para %s %s" % (paraCmd, self.jobListFh.name)

            # add options
            if self.delayTime:
                cmd = cmd+" -delayTime="+str(self.delayTime)
            if self.maxJob:
                cmd = cmd+" -maxJob="+str(self.maxJob)
            if self.maxPush:
                cmd = cmd+" -maxPush="+str(self.maxPush)
            if self.maxRam:
                cmd = cmd+" -ram="+str(self.maxRam)

            # add ssh command if headNode is set
            if self.headNode:
                if self.batchDir:
                    sshDir = self.batchDir
                else:
                    sshDir = os.getcwd()
                cmd = "ssh %s 'cd %s; para clearSickNodes; para resetCounts; %s'" % \
                    (self.headNode, sshDir, cmd)
                logging.debug("headnode set, full command is %s" % cmd)

            #os.chdir(self.batchDir)
            self._exec(cmd, stopOnError=True)

            if wait or self.runNow:
                logging.info("batch finished, batch directory %s" % self.batchDir)
            else:
                logging.info("batch created, now run 'para try' or 'para push' etc to run the jobs")

            if cleanUp:
                logging.info("Deleting back.bak, batch, para.bookmark, para.results and err")
                names = ["batch.bak", "batch", "para.bookmark"]
                for name in names:
                    os.remove(os.path.join(self.batchDir, name))
                shutil.rmtree(os.path.join(self.batchDir, "err"))

        elif self.clusterType=="sge":
            pass

def loadModule(moduleFilename):
    """ load py file dynamically  """
    # must add path to system search path first
    if not os.path.isfile(moduleFilename):
        logging.error("Could not find %s" % moduleFilename)
        sys.exit(1)
    modulePath, moduleName = os.path.split(moduleFilename)
    moduleName = moduleName.replace(".py","")
    logging.info("Loading %s" % (moduleFilename))
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

    return aMod

def testCall(text):
    """ to test this run:
    python maxRun.py maxRun.py testCall hallo
    """
    print "success, text was: ", text

def main():
    " this is the wrapper called by the submitPythonFunc() function "
    parser = optparse.OptionParser("%s pythonFile functionName param1 param2 ... - call function with params, this is supposed to be used from a batch system to call python functions")
    (options, args) = parser.parse_args()

    modName, methodName = args[:2]
    params = args[2:]

    mod = loadModule(modName)
    func = mod.__dict__.get(methodName)
    func(*params)

def test():
    r = Runner("local")
    r.submit("echo hi")
    r.finish()
    r = Runner("parasol")
    r.submit("echo hi")
    r.finish()

if __name__ == "__main__":
    main()
