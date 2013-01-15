# Copyright 2006-2012 Mark Diekhans
"""
Rules to run cluster jobs with UCSC parasol system.

A cluster batch is run in three phases
   1) setup - define jobs and create temporary files required by the batch
   2) run - run the batch on the cluster
   3) finishup - combine results of cluster jobs and remove temporary files.

A batch is defined by the path to a directory where status files
are stored indicating the completion of each of the above steps.
A temporary directory is create and used for storing batch data
and parasol files.
"""
from pycbio.exrun.Graph import Rule

class Batch(Rule):
    """A rule for running a parasol batch. A batch rule works much
    like a CmdRule, with a list of requires and produces files.
    Specific batches derive from the class and defined setup and finish up
    methods.  A directory for the batch working is created under clusterTmpRootDir.
    The field tmpDir contains the temporary directory for use by the batch.
    """
    def __init__(self, statusDir, clusterTmpRootDir, requires=None, produces=None):
        self.statusDir = statusDir
        self.clusterTmpRootDir = clusterTmpRootDir
        self.tmpDir = None
        Rule.__init__(self, statusDir, requires, produces)

        
    def setup(self):
        """Called to setup the batch, should be overridden by specific batch."""
        pass

class Parasol(Rule):
    """Construct a parasol batch rule.  A specific rule extends this class
    implementing the setup and finishup methods.  The name must be unique for
    a given exprName.  It is used to track the state of partially completed
    batches.
    """
    def __init__(self, name, requires=None, produces=None, shortName=None):
        Rule.__init__(self, name, requires=requires, produces=produces, shortName=shortName)
