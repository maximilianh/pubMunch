# module to run commands, either locally, or on parasol or sge cluster
# will autodetect cluster system

# can also run python functions instead of commands by calling itself on the cluster
# system and then calling the function (see main() and submitPythonFunc)

import os, sys, logging, shutil, types, optparse, multiprocessing, subprocess, shlex, time, imp
import pubGeneric
from os.path import isfile, join, basename, dirname, abspath

def removeFinishedProcesses(processes):
    " given a list of (commandString, process), remove those that have completed and return "
    newProcs = []
    for pollCmd, pollProc in processes:
        retCode = pollProc.poll()
        if retCode==None:
            # still running
            logging.debug("Still running %s" % pollCmd)
            newProcs.append((pollCmd, pollProc))
        elif retCode!=0:
            # failed
            raise Exception("Command %s failed" % pollCmd)
        else:
            logging.info("Command %s completed successfully" % pollCmd)
    return newProcs

def removeParasolTags(command):
    " removes the special parasol check tags from a command "
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
    return command

class Runner:
    """
    a class that runs commands or python methods on cluster or localhost (sge/parasol/local) 

    messy code.
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
        self.clusterType = clusterType
        self.logDir = logDir
        self.dryRun = dryRun
        self.runNow = runNow  # whether to wait until all commands have completed
        self.jobCount = 0

        # only relevant for SGE
        self.queue = queue

        # these are only used for parasol right now
        self.jobListFh = None
        self.delayTime = delayTime
        self.maxPush = maxPush
        self.maxJob = maxJob
        batchDir = abspath(batchDir)
        self.batchDir = batchDir
        self.maxRam = maxRam
        self.headNode = None

        self.commands = [] # for smp commands
        if not os.path.isdir(batchDir):
            logging.debug("creating dir %s" % batchDir)
            os.makedirs(batchDir)

        # the headnode string can contain the cluster type or alternatively the 
        # number of CPUs to use locally
        if headNode!=None and ":" in headNode:
            head, cType = headNode.split(":")
            if head=="localhost":
                self.clusterType="smp"
                self.maxCpu = int(cType)
            else:
                self.clusterType = cType
                self.headNode = head
        else:
            self.headNode = headNode
        
        if headNode=="localhost" or headNode=="local":
            self.headNode = None
            self.clusterType = "local"

        # auto-detect cluster type
        elif self.clusterType=="auto":
            # if we got a headNode, but no cluster system, need to ssh there
            if self.headNode is not None:
                prefixCmd = "ssh %s " % self.headNode
            else:
                prefixCmd = ""

            # run a few commands to detect cluster system, fallback to localhost
            ret = os.system("%s ps aux | grep paraHub > /dev/null" % prefixCmd)
            if ret==0:
                self.clusterType="parasol"
            else:
                ret = os.system("%s echo $SGE_ROOT | grep SGE > /dev/null" % prefixCmd)
                #sge = os.environ.get("SGE_ROOT", None)
                #if sge!=None:
                if ret ==0:
                    self.clusterType="sge"
                else:
                    self.clusterType="local"
            logging.info("Cluster type autodetect: %s" % self.clusterType)

        if self.clusterType=="parasol":
            self.jobListFname = os.path.join(self.batchDir, "jobList")
            self.jobListFh = open(self.jobListFname, "w")
            logging.info("Created jobList file in %s" % self.jobListFname)
        #elif self.clusterType.startswith("localhost:"):
            #self.maxCpu = int(self.clusterType.split(":")[1])
            #assert(self.maxCpu <= multiprocessing.cpu_count())
            #self.clusterType="smp"
        elif self.clusterType in ["sge","local","smp","localhost"]:
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
        """ submit command on sge, add command to joblist for parasol, run
        command for the "localhost" cluster system

        removes the special parasol tags {check out line, {check in line etc for SGE/localhost
        
        """

        self.jobCount += 1

        if type(command)==types.ListType:
            command = " ".join(command)

        if self.clusterType != "parasol":
            # remove parasol tags
            command = removeParasolTags(command)

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

        elif self.clusterType=="smp" or self.clusterType=="localhost":
            self.commands.append(command)

        elif self.clusterType=="parasol":
            self.jobListFh.write(command)
            self.jobListFh.write("\n")

    def submitAll(self, lines):
        " like submit, but accepts a list of lines "
        for line in lines:
            self.submit(line)
        logging.info("Submitted %d jobs" % len(lines))

    def submitPythonFunc(self, moduleName, funcName, params, jobName=None):
        """ 
        Submit a cluster job to run a python function with some parameters.

        This will call this module (see main() below) to run a python function instead of 
        an executable program. 
        The list of parameters should not contain any special characters (=only filenames)
        By default, searches moduleName in the same path as this module is located.
        """

        if not isfile(moduleName):
            libDir = os.path.dirname(__file__)
            moduleName = join(libDir, moduleName)
            assert(isfile(moduleName))
        if self.clusterType!="parasol":
            params = [removeParasolTags(p) for p in params]
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

                batchFname = join(sshDir, "batch")
                #if isfile(batchFname):
                cleanCmd = "para clearSickNodes; para resetCounts; para freeBatch; "
                #else:
                #cleanCmd = ""

                cmd = "ssh %s 'cd %s; %s %s'" % \
                    (self.headNode, sshDir, cleanCmd, cmd)
                logging.debug("headnode set, full command is %s" % cmd)

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

        elif self.clusterType=="smp":
            # adapted from http://stackoverflow.com/questions/4992400/running-several-system-commands-in-parallel
            processes = []
            for cmdCount, command in enumerate(self.commands):
                logging.info("Starting process %s" % command)
                proc =  subprocess.Popen(shlex.split(command))
                procTuple = (command, proc)
                processes.append(procTuple)
                while len(processes) >= self.maxCpu:
                    logging.debug("Waiting: totalCmd=%d, procCount=%d, cpuCount=%d, current=%d, ..." % \
                        (len(self.commands), len(processes), self.maxCpu, cmdCount))
                    time.sleep(1.0)
                    processes = removeFinishedProcesses(processes)

            # wait for all processes
            while len(processes)>0:
                time.sleep(0.5)
                processes = removeFinishedProcesses(processes)
            logging.info("All processes completed")


def loadModule(moduleFilename):
    """ load py file dynamically  """
    # must add path to system search path first
    if not os.path.isfile(moduleFilename):
        logging.error("Could not find %s" % moduleFilename)
        sys.exit(1)
    if moduleFilename.endswith(".py") or moduleFilename.endswith(".pyc"):
        modulePath, moduleName = os.path.split(moduleFilename)
        moduleName = moduleName.replace(".pyc","").replace(".py","")
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
    else:
        aMod = imp.load_source(basename(moduleFilename), moduleFilename)

    return aMod

def testCall(text):
    """ to test this run:
    python maxRun.py maxRun.py testCall hallo
    """
    print "success, text was: ", text

def main():
    " this is the wrapper called by the submitPythonFunc() function "
    parser = optparse.OptionParser("%s pythonFile functionName param1 param2 ... - call function with params, this is supposed to be used from a batch system to call python functions")
    pubGeneric.addGeneralOptions(parser)
    (options, args) = parser.parse_args()
    #pubGeneric.setupLogging(__file__, options)

    modName, methodName = args[:2]
    params = args[2:]

    mod = loadModule(modName)
    assert(mod!=None) # module not found?
    func = mod.__dict__.get(methodName)
    assert(func!=None) # function not found?
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
