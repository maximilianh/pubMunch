# Copyright 2006-2012 Mark Diekhans
import cProfile, signal

sigProfObject = None

def sigHandler(signum, frame):
    "signal handler to stop logging and terminate process"
    sigProfObject.finishUp()
    sys.stderr("Warning: profiler exiting on signal\n")
    sys.exit(1)

class Profile(object):
    """Wrapper to make adding optional support for profiling easy.
    Adds cmd options:
       --profile=profFile
       --profile-sig=signal
       --profile-lines

    Serving suggestion:
        parser = OptionParser(usage=CmdOpts.usage)
        self.profiler = Profile(parser)
        ...
        (opts, args) = parser.parse_args()
        ...
        self.profiler.setup(opts)
    
    at the end of the program:
        xxx.profiler.finishUp()

    use the program profStats to create reports.
    """

    def __init__(self, cmdParser):
        cmdParser.add_option("--profile", dest="profile", action="store",
                             default=None, type="string",
                             help="enable profiling, logging to this file")
        cmdParser.add_option("--profile-signal", dest="signal", action="store",
                             default=None, type="int",
                             help="specify signal number that will stop logging and exit program")
        self.profiler = None
        self.logFile = None
        self.signum = None

    def setup(self, opts):
        """initializing profiling, if requested"""
        if opts.profile == None:
            if opts.signal != None:
                raise Exception("can't specify --profile-signal without --profile")
        else:
            if opts.signal != None:
                global sigProfObject
                sigProfObject = self
                self.signum = opts.signal
                signal.signal(self.signum, sigHandler)
            self.logFile = opts.profile
            self.profiler = cProfile.Profile()
            self.profiler.enable()

    def finishUp(self):
        "if profiling is enabled, stop and close log file"
        if self.profiler != None:
            self.profiler.disable()
            if self.signum != None:
                signal.signal(self.signum, signal.SIG_IGN)
                sigProfObject = None
                self.signum = None
            self.profiler.dump_stats(self.logFile)
                
