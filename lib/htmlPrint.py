# library for convenience functionc related to html ouput
import sys, codecs, urllib.request, urllib.error, urllib.parse, urllib.request, urllib.parse, urllib.error, cgi, doctest, logging, re, os
try:
    from BeautifulSoup import BeautifulStoneSoup
except:
    #sys.stderr.write("warning html.py: BeautifulSoup not installed.\n")
    pass

try:
    from lxml import etree
except:
    pass


# *** return only the urls
def aniGeneUrl(geneId):
    return 'http://crfb.univ-mrs.fr/aniseed/molecule-gene.php?name=%s' % geneId
def entrezUrl(accNo):
    return 'http://www.ncbi.nlm.nih.gov/sites/entrez?db=Gene&amp;term=%s' % accNo
def ensGeneUrl(geneId, orgName="Homo_sapiens"):
    orgName=orgName.replace(" ", "_")
    return 'http://sep2009.archive.ensembl.org/%s/geneview?gene=%s;db=core' % (orgName, geneId)
def genbankUrl(accId):
    return 'http://www.ncbi.nlm.nih.gov/sites/entrez?db=nuccore&amp;term=%s' % accId
def ncbiTaxonUrl(taxId):
    return 'http://www.ncbi.nlm.nih.gov/sites/entrez?db=taxonomy&amp;term=%s' % str(taxId)
def pmcFulltextXmlUrl(pmcId):
    return 'http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=GetRecord&identifier=oai:pubmedcentral.nih.gov:%s&metadataPrefix=pmc' % str(pmcId)

def ensemblAutoAttachUrl(genome, chrom, start, end, ourDasUrl):
    coords = "%s:%d-%d" % (chrom, start, end)
    # example: "http://www.ensembl.org/Homo_sapiens/Location/View?g=ENSG00000012048;contigviewbottom=das:http://www.ensembl.org/das/Homo_sapiens.NCBI36.transcript=labels"
    url = "http://sep2009.archive.ensembl.org/"+genome+"/Location/View?r=%s;contigviewbottom=das:%s=labels" %(coords, ourDasUrl)
    return url
def ucscTrackUrl(hgsid, chrom, start, end, server="http://genome.ucsc.edu"):
    return server+"/cgi-bin/hgTracks?hgsid=%s&position=%s:%s-%s" % (hgsid, chrom, start, end)

def pubmedUrl(pmid):
    return 'http://www.ncbi.nlm.nih.gov/pubmed/%s' % (pmid)
def pmcUrl(pmcId):
    return 'http://www.pubmedcentral.nih.gov/articlerender.fcgi?artid=%s' % (pmcId)

# *** return complete links = with <a> tags around and a default text

def pubmedLink(pmid, text=None):
    if not text:
        text=pmid
    return '<a href="http://www.ncbi.nlm.nih.gov/pubmed/%s">%s</a>' % (pmid, text)

def pmcLink(articleId, text=None):
    if not text:
        text=articleId
    artStr = str(articleId)
    return '<a href="http://www.pubmedcentral.nih.gov/articlerender.fcgi?artid=%s">%s</a>' % (artStr, text)
def ensGeneLink(geneId, orgName="Homo_sapiens"):
    return '<a href="http://sep2009.archive.ensembl.org/%s/geneview?gene=%s;db=core">%s</a>' % (orgName, geneId, geneId)
def geneCardsLink(symbol):
    return '<a href="http://www.genecards.org/cgi-bin/cardsearch.pl?search=disease&symbols=%s#MINICARDS">%s</a>' % (symbol, symbol)
def aniGeneLink(geneId):
    return '<a href="http://crfb.univ-mrs.fr/aniseed/molecule-gene.php?name=%s">%s</a>' % (geneId, geneId)
def aniInsituPageLink(insituId):
    return '<a href="http://crfb.univ-mrs.fr/aniseed/insitu.php?id=%s">%s</a>' % (insituId, insituId)
def aniISHLink(geneId):
    return '<a href="http://crfb.univ-mrs.fr/aniseed/insitu-result.php?target=%s&BOOLmut=3&BOOLmanip=2&MOLtype=2&Order=DEV_STAGE_ID">%s</a>' % (geneId, geneId)
def genbankLink(accId):
    return '<a href="http://www.ncbi.nlm.nih.gov/entrez/query.fcgi?cmd=Search&amp;db=Nucleotide&term=%s&amp;doptcmdl=GenBank">%s</a>' %(accId, accId)

def ensGenomeLink(orgName, chrom, start, end):
    coords = '%s:%d-%d' % (chrom, start, end)
    return '<a href="http://www.ensembl.org/%s/Location/View?r=%s">%s</a>' % (orgName, coords, coords)

def ucscGenomeLink(baseUrl, db, pos, hgsid=None, desc=None):
    hgsidStr=""
    if desc==None:
       desc = pos
    if hgsid!=None:
        hgsidStr = "&hgsid=%s" % hgsid
    return '<a href="%s/cgi-bin/hgTracks?db=%s&position=%s%s">%s</a>' % (baseUrl, db, pos, hgsidStr, desc)

def ucscCustomTrackUrl(db, pos, customTrackUrl, server="http://genome.ucsc.edu"):
    return server+"/cgi-bin/hgTracks?db=%s&position=%s&hgt.customText=%s" % (db, pos, customTrackUrl)

def ucscCustomTrackLink(description, db, pos, customTrackUrl, server="http://genome.ucsc.edu"):
    return '<a href="%s">%s</a>' % (ucscCustomTrackUrl(db, pos, customTrackUrl, server), description)

def ucscMafLink(text, baseUrl, db, track, chrom, start, end):
    return '<a href="%s/cgi-bin/hgc?o=%d&t=%d&g=%s&c=%s&l=%d&r=%d&db=%s">%s</a>' % (baseUrl, start, end, track, chrom, start, end, db, text)
def pmcFulltextXmlLink(pmcId):
    return '<a href="%s">PMC%s</a>' % (pmcFulltextXmlUrl(pmcId), str(pmcId))

def ucscHgsid(db, server="http://genome.ucsc.edu"):
    """ get a new hgsid from ucsc """
    """ db is a ucsc database as a string, e.g. hg18"""
    #print("Requesting a new hgsid from UCSC")
    data = urllib.request.urlopen(server+"/cgi-bin/hgCustom?db=%s" % db)
    for line in data:
        if line.startswith('<INPUT TYPE=HIDDEN NAME="hgsid" VALUE="') or line.startswith("<INPUT TYPE=HIDDEN NAME='hgsid' VALUE=") :
            line = line.strip()
            line = line.replace('<INPUT TYPE=HIDDEN NAME="hgsid" VALUE="', '')
            line = line.replace('"><TABLE BORDER=0>', '')
            line = line.replace("<INPUT TYPE=HIDDEN NAME='hgsid' VALUE='", '')
            line = line.replace("'>", '')
            #print("New hgsid %s" % line)
            return line
    #sys.stderr.write("error in UCSC web parser, write to maximilianh@gmail.com to get this fixed")
    print("error in UCSC web parser, write to maximilianh@gmail.com to get this fixed")

def ucscUpload(db, data, hgsid=None, server="http://genome.ucsc.edu", name="User track", description="User track", visibility="1", clade="deuterostome", organism="C. intestinalis"):
    """ adds data as a user track, creates and returns session id if hgsid is None, returns None on error """
    """ db is a string like hg18, data is your custom track as one big string (including the newlines)"""
    #log("Uploading %d lines to UCSC" % (data.count("\n")+1))

    if not data.startswith("track"):
        data = 'track name="%s" description="%s" visibility=%s\n' % (name, description, visibility) + data

    if hgsid==None:
        hgsid = ucscHgsid(db, server)

    vars = {}
    vars["hgsid"]=hgsid
    vars["clade"]=clade
    vars["org"]=organism
    vars["db"]=db
    vars["hgct_customText"]=data
    vars["Submit"]="Submit"
    html = urllib.request.urlopen(server+"/cgi-bin/hgCustom", urllib.parse.urlencode(vars))
    html = html.readlines()
    for l in html:
        if l.find("Manage Custom Tracks")!=-1:
            return hgsid
    print("ERROR: Could not upload custom track into UCSC server at %s<br>\n" % server)
    print("Offending data was:<br>\n")
    print(data)
    print("Error Message was:<br>\n")
    print("\n".join(html))
    return None

def getStylesheet(name):
    " return a stylesheet "
    if name=="dent":
        stylesheet="""
body { 
    font: 10pt/11pt sans-serif; 
    color: #555753; 
    margin-left: 0em;
}
p { 
    font: 10pt/12pt sans-serif; 
    margin-top: 1em; 
    margin-left: 1em; 
    text-align: justify;
}
p.five{
    width:50em;
}
p.threeFive{
    width:35em;
}
p.twoFive{
    width:25em;
}
h1{
  font-family: sans-serif;
  font-weight:bold;
  font-size:40px;
}
h2 {
    font: bold normal 14pt sans-serif; 
    letter-spacing: 1px; 
    color: #555753;
}
h3 { 
    font: bold 12pt sans-serif; 
    letter-spacing: 1px; 
    margin-left: 4px; 
    margin-bottom: 4px; 
    color: #7D775C;
}
h4 { 
    font: bold 11pt sans-serif; 
    letter-spacing: 1px; 
    margin-left: 4px; 
    margin-bottom: 4px; 
    color: #5e5944;
}

#container {
  width: 800px;
  margin-bottom: 10px;
  margin-left: auto;
  margin-right: auto;
  padding: 0em;
  background-color: #FFFFFF;
}
#content {
  background-color: #FFFFFF;
  padding: 0em;
  margin-top: -1em;
  margin-left: 13.5em;
  margin-right: 12.5em;
}
#banner {
  background-color: #FFFFFF;
  text-align: right;
  padding: .15em;
  margin: 0em;
}
#left {
  float: left;
  width: 10em;
  margin: 0em;
  padding: 0em;
}
.navcol {
  -moz-border-radius: 15px;
  border-radius: 15px;
  background: #EFEFFB;
  position: relative;
  margin-top: 2.9em;
  padding: .15em;
  width:12.5em;
}
.navcol a{
    font-size: 90%;
    font-weight:bold;
    color: #969696;
    text-decoration: none;
}
.navcol a:hover {
    color: #0062d3;
}
#footer {
  clear: both;
  margin: 0em;
  padding: 0em;
  text-align: right;
  background-color:#FFFFFF;
}
tr.alt{
    background-color:#cbe1f3;
}"""
    elif name=="dyndrive":
        stylesheet = """
/*Credits: Dynamic Drive CSS Library */
/*URL: http://www.dynamicdrive.com/style/ */
/* <style type="text/css"> */


.urbangreymenu{
    width: 150px; /*width of menu*/
    position:fixed;
    border: 0px;
    z-index:100;
    margin-left: -180px;
}

.urbangreymenu .headerbar{
    font: bold 13px Verdana;
    color: white;
    /* background: #606060 url(media/arrowstop.gif) no-repeat 8px 6px; /*last 2 values are the x and y coordinates of bullet image*/ 
    background: #606060;  /*last 2 values are the x and y coordinates of bullet image*/ 
    margin-bottom: 0; /*bottom spacing between header and rest of content*/
    /* text-transform: uppercase; */
    padding: 7px 0 7px 8px; /*31px is left indentation of header text*/
}

.urbangreymenu ul{
    list-style-type: none;
    margin: 0;
    padding: 0;
    margin-bottom: 0; /*bottom spacing between each UL and rest of content*/
}

.urbangreymenu ul li{
    padding-bottom: 2px; /*bottom spacing between menu items*/
}

.urbangreymenu ul li a{
    font: normal 12px Arial;
    color: black;
    background: #E9E9E9;
    display: block;
    padding: 5px 0;
    line-height: 17px;
    padding-left: 8px; /*link text is indented 8px*/
    text-decoration: none;
}

.urbangreymenu ul li a:visited{
    color: black;
}

.urbangreymenu ul li a:hover{ /*hover state CSS*/
    color: black;
    background: #CCCCCC;
}

/*
Design by Free CSS Templates
http://www.freecsstemplates.org
Released for free under a Creative Commons Attribution 2.5 License
*/

/* Elements */

body {
        /* background: #6E6E6E url(img1.jpg) repeat-x; */
        background: #EEEEEE; 
        margin: 0px;
    margin-left: 190px; 
        text-align: left;
        font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
        font-size: smaller; 
        color: #333333;
    width: 800px;
}

h1 {
        color: #FFFFFF;
}

h2, h3 {
        margin-top: 10px;
}

h4, h5, h6 {
}

p, ol, ul, dl, blockquote {
}


/* tables */

table {
    table-layout:fixed;
}

th { background : #BBBBBB; 
}

tr { background : #DDDDDD;
     font-size: smaller;
}

a:link {
        color: #000099;
        text-decoration: none;
}

a:hover {
        cursor: hand;
        color: #990099;
        text-decoration: none;
}
a:visited {
        color: #551a8b;
        text-decoration: none;
}


/* Header */

#header {
        width: 600px;
        height: 200px;
        margin: 0px auto;
        /* background: url(img2.jpg); */
}

#header h1 {
        margin: 0px;
        padding: 100px 0 0 60px;
        font-size: 42px;
        letter-spacing: -2px;
}

#header h2 {
        margin: 0;
        padding: .1em 0 0 60px;
        font-size: 16px;
        letter-spacing: -1px;
        color: #666666;
}

#header a {
        text-decoration: none;
        color: #FFFFFF;
}

/* Menu */

#menu {
        width: 600px;
        height: 30px;
        margin: 0px auto;
}

#menu ul {
        margin: 0px;
        padding: 0px;
        list-style: none;
}

#menu li {
        display: inline;
}

#menu a {
        display: block;
        float: left;
        width: 100px;
        padding: 7px 0px;
        text-align: center;
        text-decoration: none;
        text-transform: uppercase;
        font-weight: bold;
        background: #EEEEEE;
}

#menu a:hover {
        background: #CCCCCC;
}

/* Content */

#content {
        background: #FFFFFF;
        width: 600px;
        margin: 0px auto;
        padding: 2px 0px 0px 0px;
}

#colFull {
        width: 550px;
        margin-top: 20px;
        margin-bottom: 20px;
        padding-right: 15px;
        padding-left: 25px;

}

#colOne {
        float: right;
        width: 360px;
        margin-top: 20px;
        padding-right: 20px;
}

#colTwo {
        float: left;
        width: 180px;
        margin-top: 20px;
        padding-right: 20px;
        padding-left: 20px;
}

#colTwo ul {
        margin-left: 0px;
        padding-left: 0px;
        list-style-position: inside;
}

#content h1 {
        padding: 5px 0px 5px 5px;
        color: #2D2D2D;
}

#content h2 {
        padding: 5px 0px 5px 5px;
        text-transform: uppercase;
        font-size: 16px;
        color: #2D2D2D;
        border-bottom: 1px dashed;
}

#content h3 {
        padding: 5px 0px 5px 5px;
        color: #6C6C6C;
}
/* Footer */

#footer {
        width: 600px;
        margin: 0px auto;
        padding: 3px 0px;
        height: 50px;
        background: #EEEEEE;
}

#footer p {
        margin: 0px;
        padding-top: 15px;
        text-align: center;
        font-size: 11px;
        color: #999999;
}

#footer a {
        color: #666666;
}

#footer a:hover {
        color: #333333;
}
    """
    return stylesheet

class htmlWriter:
    def __init__(self,fname=None,fh=None):
        if fname=="stdout" or (fname==None and fh==None):
            #codecs.getwriter('utf8')(sys.stdout.buffer)
            self.f = sys.stdout
        elif fname:
            self.f = open(fname, "w")
        elif fh:
            self.f = fh

    def startCgi(self, contentType="text/html; charset=utf-8", addLines=None):
        self.writeLn ("Content-type: %s" % contentType)
        if addLines:
            for l in addLines:
                self.writeLn(l)
        self.writeLn("")

    def write(self, text):
        #self.f.write(text.encode("latin1", 'replace'))
        self.f.write(text.encode("utf8", 'replace'))

    def writeLn(self, str):
        if str==None:
            str="None"
        self.write(str)
        self.write("\n")

    def head(self, title, stylesheet=None, styleString=None, scripts=None, metaTags=[]):
        self.f.write("""
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" >
<title>"""+title+"""</title>""")
        if stylesheet!=None:
            self.f.write("""<link href="%s" rel="stylesheet" type="text/css" >\n""" % stylesheet)
        if styleString!=None:
            self.f.write("""<style type="text/css">%s\n</style>\n""" % styleString)
        if scripts!=None:
            for script in scripts:
                self.f.write("""<script type="text/javascript" src="%s"></script>\n""" % script)
        for meta, val in metaTags:
            self.f.write("""<meta http-equiv="%s" content="%s">\n""" % (meta, val))
            
        self.f.write ("""</head>\n""")

    def endHtml(self):
        self.f.write("\n</body>\n</html>")
        self.f.close()

    def insertMenu(self, menu):
        """ menu: dict with title => list of (url, description)"""
        self.writeLn("")
        self.writeLn('<div class="urbangreymenu">')
        for title, titleData in menu:
            self.writeLn('<h3 class="headerbar">%s</h3>' % title)
            self.writeLn("<ul>")
            for desc, url in titleData:
                self.writeLn('<li><a href="%s">%s</a></li>' % (url, desc))
            self.writeLn('</ul>')
        self.writeLn("</div>")
        self.writeLn("")

    def startBody(self, title=None, menu=None):
        self.writeLn("<body>")
        if menu:
            self.insertMenu(menu)
        if title:
            self.f.write("""<h2>"""+title+"""</h2>\n""")

    def img(self, url, alt, width=None, height=None):
        if width:
            attr = 'width="%s"' % str(width)
        if height:
            attr += ' height="%s"' % str(height)
        self.f.write('<img src="%s" alt="%s" %s/>\n' % (url, alt, attr))

    def h2(self, text):
        str=('<h2>'+text+'</h2>\n')
        self.write(str)

    def h4(self, text):
        str=('<h4>'+text+'</h4>\n')
        self.write(str)

    def linkList(self, list):
        self.f.write('Index:<ul>\n')
        for pair in list:
            desc, name = pair
            self.f.write('    <li>\n')
            self.f.write('    <a href="#%s">%s</a>\n' % (name, desc))
        self.f.write('</ul>\n')

    ### TABLES

    def startTable(self, widths, headers, bgcolor=None, cellspacing=2, cellpadding=3, tblClass=None, headClass=None):
        options = ""
        if tblClass!=None:
            options = options+' class="%s"' % tblClass
        if bgcolor!=None:
            options = options+' bgcolor="%s"' % bgcolor

        self.f.write('\n<table  border="0" cellspacing="%d"  cellpadding="%d"  %s>\n' % (cellspacing, cellpadding, options) )

        if len(widths)>0:
            self.f.write("<colgroup>\n")
            for width in widths:
                self.f.write("  <col width='%s'>\n" % (str(width)))
            self.f.write("</colgroup>\n\n")

        if len(headers)>0:
            headOpts = ""
            if headClass!=None:
                headOpts = 'class="%s"' % headClass
            self.f.write("  <thead %s>\n" % headOpts)
            self.f.write("  <tr>\n")
            for header in headers:
                self.f.write('    <th align="left">%s</th>\n' % (header))
            self.f.write("  </tr>\n")
            self.f.write("  </thead >\n")

    def endTable(self):
        self.f.write("</table>\n")

    def td(self, str, colour=None):
        self.tableData(str, colour)

    def tableData(self, str, colour=None):
        if colour==None:
            self.f.write('<td>%s</td>\n' % str)
        else:
            self.f.write('<td bgcolor="%s">%s</td>\n' % (colour, str))

    def startTr(self, bgcolor=None):
        if bgcolor==None:
            self.f.write('<tr valign="top">\n')
        else:
            self.f.write('<tr bgcolor="%s" valign="top">\n' % bgcolor)

    def endTr(self):
        self.f.write("</tr>\n")

    def startTd(self):
        self.f.write("<td>\n")

    def endTd(self):
        self.f.write("</td>\n")

    def tableRow(self, cellList, bgColour=None, colorList=None):
        self.startTr(bgColour)

        for i in range(0, len(cellList)):
            if colorList!=None:
                col=colorList[i]
            else:
                col=None
            cell = cellList[i]
            self.tableData(cell, col)
        self.endTr()

    ## TEXT FORMATTING 
    def link(self, url, text):
        self.write('<a href="%s">%s</a>' % (url, text))

    def linkStr(self, url, text):
        return '<a href="%s">%s</a>' % (url, text)

    def anchor(self, name):
        self.f.write('<a name="%s"></a>' % name)

    def h3(self, str, anchor=None):
        if anchor!=None:
            self.f.write('<a name="%s"></a>\n' % anchor)
        self.f.write('<hr>\n<h3>%s</h3>\n' % str)

    def small(self,str):
        self.f.write('<small>%s</small>' % str)

    def b(self, str):
        self.f.write('<b>%s</b>' % str)

    def br(self):
        self.f.write("<br>\n")

    def pre(self, text):
        self.f.write("<pre>%s</pre>\n" % text)

    def p(self):
        self.f.write("<p>\n")

    def centerStart(self):
        self.f.write("<center>\n")

    def centerEnd(self):
        self.f.write("</center>\n")

    def startUl(self):
        self.f.write("<ul>\n")

    def li(self, text):
        self.f.write("<li>%s</li>\n" % text)

    def endUl(self):
        self.f.write("</ul>\n")


    ## FORMS 

    def startForm(self, action, method="get"):
        self.writeLn('<form name="input" action="%s" method="%s">\n' % (action, method))

    def formInput(self, type, name, size=None, value=None):
        addStr=""
        if size:
            addStr+='size="%d"' % size
        if value:
            addStr+='value="%s"' % value

        self.writeLn('<input type="%s" name="%s" %s />\n' % (type, name, addStr))

    def formInputText(self, name, size=None):
        self.formInput("text", name, size)

    def formInputSubmit(self, name):
        self.formInput("submit", name, value=name)

    def startTextArea(self, name, rows=3, cols=30, id=None):
        opt = ""
        if id!=None:
            opt = 'id="%s"' % id
        self.writeLn('<textarea name="%s" rows="%d" cols="%d" %s>\n' % (name, rows, cols, opt))

    def endTextArea(self):
        self.writeLn('</textarea>\n')

    def formInputReset(self, name):
        self.formInput("reset", name, value=name)

    def endForm(self):
        self.writeLn('</form>\n')

    ## LINKS TO EXTERNAL RESOURCES
    def ensGeneLink(self, orgName, geneId):
        return ensGeneLink(orgName, geneId)

    def ucscGenomeUrl(self, baseUrl, db, pos):
        return '%s/cgi-bin/hgTracks?db=%s&position=%s' % (baseUrl, db, pos)

    def zfinGeneLink(self, geneId, title=None):
        if title==None:
            title=""
        else:
            title = ' title="%s" ' % title
        return '<a href="http://zfin.org/cgi-bin/webdriver?MIval=aa-markerview.apg&OID=%s" %s>%s</a>' % (geneId, title, geneId)

    def geneCardsLink(self, symbol):
        return geneCardsLink(symbol)

    def brainAtlasLink(self, mouseEntrezIds):
        abaItems = ["entrezgeneid:=" + i for i in mouseEntrezIds]
        query = " OR ".join(abaItems)
        ids = ", ".join(mouseEntrezIds)
        return '<a href="http://www.brain-map.org/search.do?findButton.x=1&queryText=%s">%s</a>' % (query, ids)

    def zfinInsituLink(self, geneId, desc=None, title=None):
        if desc==None:
            desc=geneId
        if title!=None:
            titleStr=' title="%s" ' % title
        else:
            titleStr=""
        return '<a href="http://zfin.org/cgi-bin/webdriver?MIval=aa-xpatselect.apg&query_results=true&xpatsel_geneZdbId=%s" %s">%s</a>' % (geneId, titleStr, desc)

    def ghostGeneUrl(self, geneId):
        return 'http://ghost.zool.kyoto-u.ac.jp/cgi-bin3/txtgetr2.cgi?%s' % geneId

    def ghostInSituUrl(self, geneId):
        return 'http://ghost.zool.kyoto-u.ac.jp/cgi-bin3/photoget2.cgi?%s' % geneId

    def aniseedInSituUrl(self, geneId):
        return 'http://crfb.univ-mrs.fr/aniseed/insitu-result.php?target=%s&BOOLmut=3&BOOLmanip=2&MOLtype=2&Order=DEV_STAGE_ID' % geneId

    def ensemblMCUrl(self, baseName, compName, basePair, compPair):
        """ return string with html-link to ensembl multicontigview from two genes given genes and organism names """
        urlMask = "http://www.ensembl.org/%s/multicontigview?s1=%s;w=%d;c=%s:%d:1;w1=%d;c1=%s:%d:1;action=%s;id=1"  
        baseSize = (basePair.right.end - basePair.left.start) * 2
        baseChrom = basePair.left.chrom.replace("chr","")
        basePos = basePair.left.start
        compChrom = compPair.left.chrom.replace("chr","")
        compPos = compPair.left.start
        if compPair.left.start - compPair.right.start < 0:
            action="out"
            compSize = (compPair.right.end - compPair.left.start) * 2
        else:
            action="flip"
            compSize = (compPair.left.end - compPair.right.start) * 2
        url = urlMask % (baseName, compName, baseSize, baseChrom, basePos, compSize, compChrom, compPos, action)
        text = "(Multicontigv.)"
        urlStr = '<a href="%s">%s</a>\n' % (url, text)
        return urlStr

## CGI
    def gotCgiVariables(self):
        self.cgiVarsRaw = cgi.FieldStorage()

        self.cgiVars={}
        for var in self.cgiVarsRaw:
            self.cgiVars[var]=self.cgiVarsRaw[var].value

        return len(self.cgiVars)!=0

def HTMLEntitiesToUnicode(text):
    """Converts HTML entities to unicode.  For example '&amp;' becomes '&'."""
    text = str(BeautifulStoneSoup(text, convertEntities=BeautifulStoneSoup.ALL_ENTITIES))
    return text

def unicodeToHTMLEntities(text):
    """Converts unicode to HTML entities.  For example '&' becomes '&amp;'."""
    text = cgi.escape(text).encode('ascii', 'xmlcharrefreplace')
    return text

if __name__ == "__main__":
    import doctest
    doctest.testmod()

