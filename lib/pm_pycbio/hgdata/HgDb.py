# Copyright 2006-2012 Mark Diekhans
"""Connect to UCSC genome database using info in .hg.conf """
from pm_pycbio.hgdata.HgConf import HgConf
import MySQLdb, warnings

# turn most warnings into errors except for those that are Notes
# from `drop .. if exists'.  This could have also been disabled
# with a set command.
warnings.filterwarnings('error', category=MySQLdb.Warning)
warnings.filterwarnings("ignore", "Unknown table '.*'")
warnings.filterwarnings("ignore", "Can't drop database '.*'; database doesn't exist")


def connect(db=None,  confFile=None):
    """connect to genome mysql server, using confFile or ~/.hg.conf"""
    conf = HgConf.obtain(confFile)
    return MySQLdb.Connect(host=conf["db.host"], user=conf["db.user"], passwd=conf["db.password"], db=db)
    
