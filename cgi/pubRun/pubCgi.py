import cgitb
cgitb.enable()
import os.path
# libs one needs to install
import MySQLdb

# ---- CONFIG -----
HGCONF = "../cgi-bin/hg.conf"

def parseHgConf(fname=HGCONF, conf={}):
    " parse HgConf and return as dict "
    for line in open(fname):
        line = line.strip()
        if len(line)==0:
            continue
        if line.startswith("#"):
            continue
        if line.startswith("include "):
            relFname = line.split()[1]
            absFname = os.path.join(os.path.dirname(fname), relFname)
            parseHgConf(absFname, conf)
            continue
        key, val = string.split("=", 1)
        conf[key]=val
    return conf

def sqlConnect():
    """ parse hg.conf and connect to default host with user and pwd """
    conf = parseHgConf()
    host = conf["db.host"]
    user = conf["db.user"]
    passwd = conf["db.password"]
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd,db="publications")
    return conn

def printHead(h, title, metaTags=[]):
    h.head(title, stylesheet="http://genome.ucsc.edu/style/HGStyle.css", scripts=["http://ajax.googleapis.com/ajax/libs/jquery/1.8.3/jquery.min.js"], metaTags=metaTags)


def topBar(h):
    " print browser top bar "
    h.writeLn("""
    <!-- start top bar copied from browser -->
    <TABLE BGCOLOR="#000000" CELLPADDING="1" CELLSPACING="1" WIDTH="100%">
    <TR BGCOLOR="#2636D1"><TD VALIGN="middle">
    <TABLE BORDER=0 CELLSPACING=0 CELLPADDING=0 BGCOLOR="#2636D1" class="topbar">
    <TR><TD VALIGN="middle"><FONT COLOR="#89A1DE">
      <A HREF="http://genome.ucsc.edu/index.html" class="topbar">Home</A>-
      <A HREF="http://genome.ucsc.edu/cgi-bin/hgGateway" class="topbar">Genomes</A>- 
      <A HREF="http://genome.ucsc.edu/cgi-bin/hgBlat?command=start" class="topbar">Blat</A>- 
      <A HREF="http://genome.ucsc.edu/cgi-bin/hgTables?command=start" class="topbar">Tables</A>- 
      <A HREF="http://genome.ucsc.edu/cgi-bin/hgNear" class="topbar">Gene Sorter</A>- 
      <A HREF="http://genome.ucsc.edu/cgi-bin/hgPcr?command=start" class="topbar">PCR</A>- 
      <A HREF="http://genome.ucsc.edu/cgi-bin/hgSession?hgS_doMainPage=1" class="topbar">Session</A>-
      <A HREF="http://genome.ucsc.edu/FAQ/" class="topbar">FAQ</A>- 
      <A HREF="http://genome.ucsc.edu/goldenPath/help/hgTracksHelp.html" class="topbar">Help</A>
    </FONT></TD></TR>
    </TABLE>
    </TD></TR>
    </TABLE>
    <!-- end topbar -->
    <P>
    """)

