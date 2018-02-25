# Copyright 2006-2012 Mark Diekhans
"""Create a frameset that that is a directory of locations in the genome
browser.
"""

from pm_pycbio.hgbrowser.Coords import Coords
from pm_pycbio.html.HtmlPage import HtmlPage
from pm_pycbio.sys import fileOps

defaultStyle = """
TABLE, TR, TH, TD {
    white-space: nowrap;
    border: solid;
    border-width: 1px;
    border-collapse: collapse;
}
"""

class SubRows(object):
    """Object used to specify a set of sub-rows.  Indicates number of columns
    occupied, which is need for laying out.
    """
    def __init__(self, numCols):
        self.numCols = numCols
        self.rows = []

    def addRow(self, row):
        assert(len(row) == self.numCols)
        self.rows.append(row)

    def getNumRows(self):
        return len(self.rows)

    def toTdRow(self, iRow):
        """return row of cells for the specified row, or one covering multiple
        columns if iRow exceeds the number of rows"""
        if iRow < len(self.rows):
            return "<td>" + "<td>".join([str(c) for c in self.rows[iRow]])
        elif self.numCols > 1:
            return "<td colspan=%d>" % self.numCols
        else:
            return "<td>"

class Entry(object):
    "entry in directory"
    __slots__= ("row", "key", "cssClass", "subRowGroups")

    def __init__(self, row, key=None, cssClass=None, subRows=None):
        """Entry in directory, key can be some value(s) used in sorting. The
        row should be HTML encoded.  If subRows is not None, it should be a SubRow
        object or list of SubRow objects, used to produce row spanning rows for
        contained in this row."""
        self.row = tuple(row)
        self.key = key
        self.cssClass = cssClass
        self.subRowGroups = None
        if subRows != None:
            if isinstance(subRows, SubRows):
                self.subRowGroups = [subRows]
            else: 
                self.subRowGroups = subRows

    def __numSubRowGroupCols(self):
        n = 0
        if self.subRowGroups != None:
            for subRows in self.subRowGroups:
                n += subRows.numCols
        return n

    def __numSubRowGroupRows(self):
        n = 0
        if self.subRowGroups != None:
            for subRows in self.subRowGroups:
                n = max(n, subRows.getNumRows())
        return n

    def numColumns(self):
        "compute number of columns that will be generated"
        return len(self.row) + self.__numSubRowGroupCols()

    def toHtmlRow(self):
        numSubRowRows = self.__numSubRowGroupRows()
        h = ["<tr>"]
        if numSubRowRows > 1:
            td = "<td rowspan=\""+str(numSubRowRows) + "\">"
        else:
            td = "<td>"
        for c in self.row:
            h.append(td + str(c))
        if numSubRowRows > 0:
            for subRows in self.subRowGroups:
                h.append(subRows.toTdRow(0))
        h.append("</tr>\n")

        # remaining rows
        for iRow in xrange(1, numSubRowRows):
            h.append("<tr>\n")
            for subRows in self.subRowGroups:
                h.append(subRows.toTdRow(iRow))
            h.append("</tr>\n")
        return "".join(h)

class BrowserDir(object):
    """Create a frameset and collection of HTML pages that index one or more
    genome browsers.
    """

    def __init__(self, browserUrl, defaultDb, colNames=None, pageSize=50,
                 title=None, dirPercent=15, below=False, pageDesc=None,
                 tracks=None, initTracks=None, style=defaultStyle, numColumns=1, customTrackUrl=None):
        """The tracks arg is a dict of track name to setting, it is added to
        each URL and the initial setting of the frame. The initTracks arg is
        similar, however its only set in the initial frame and not added to
        each URL.
        A pageSize agr of None creates a single page. If numColumns is greater than 1
        create multi-column directories.
        """
        self.browserUrl = browserUrl
        if self.browserUrl.endswith("/"):
            self.browserUrl = self.browserUrl[0:-1] # drop trailing `/', so we don't end up with '//'
        self.defaultDb = defaultDb
        self.colNames = colNames
        self.pageSize = pageSize
        self.title = title
        self.dirPercent = dirPercent
        self.below = below
        self.numColumns = numColumns
        self.pageDesc = pageDesc
        self.entries = []
        self.style = style
        self.customTrackUrl = customTrackUrl
        self.trackArgs = self.__mkTracksArgs(tracks)
        self.initTrackArgs = self.__mkTracksArgs(initTracks)
        if customTrackUrl != None:
            self.tracksArgs += "&hgt.customText=" + self.customTrackUrl
            

    def __mkTracksArgs(self, initialTracks):
        if (initialTracks == None) or (len(initialTracks) == 0):
            return ""
        l = []
        for t in initialTracks:
            l.append(t + "=" + initialTracks[t])
        return "&" + "&".join(l)
        
    def mkDefaultUrl(self):
        return self.browserUrl + "/cgi-bin/hgTracks?db=" + self.defaultDb + "&position=default" + self.initTrackArgs  + self.trackArgs

    def mkUrl(self, coords):
        url = self.browserUrl + "/cgi-bin/hgTracks?db="
        if coords.db != None:
            url += coords.db
        else:
            url += self.defaultDb
        url += "&position=" + str(coords) + self.trackArgs
        return url

    def mkAnchor(self, coords, text=None):
        if text == None:
            text = str(coords)
        return "<a href=\"" + self.mkUrl(coords) + "\" target=browser>" + text + "</a>"
        
    def addRow(self, row, key=None, cssClass=None, subRows=None):
        """add an encoded row, row can be a list or an Entry object"""
        if not isinstance(row, Entry):
            row = Entry(row, key, cssClass, subRows)
        self.entries.append(row)

    def add(self, coords, name=None):
        """add a simple row, linking to location. If name is None, it's the
        location """
        if name == None:
            name = str(coords)
        row = [self.mkAnchor(coords, name)]
        self.addRow(row, key=coords)

    def sort(self, cmpFunc=cmp, reverse=False):
        "sort by the key"
        self.entries.sort(cmp=lambda a,b: cmpFunc(a.key, b.key), reverse=reverse)

    def __mkFrame(self, title=None, dirPercent=15, below=False):
        """create frameset as a HtmlPage object"""

        if below:
            fsAttr = "rows=%d%%,%d%%" % (100-dirPercent, dirPercent)
        else:
            fsAttr = "cols=%d%%,%d%%" % (dirPercent, 100-dirPercent)
        pg = HtmlPage(title=title, framesetAttrs=(fsAttr,))

        fdir = '<frame name="dir" src="dir1.html">'
        fbr = '<frame name="browser" src="%s">' % self.mkDefaultUrl()
        if below:
            pg.add(fbr)
            pg.add(fdir)
        else:
            pg.add(fdir)
            pg.add(fbr)
        return pg

    def __getPageLinks(self, pageNum, numPages, inclPageLinks):
        html = []
        # prev link
        if pageNum > 1:
            html.append("<a href=\"dir%d.html\">prev</a>" % (pageNum-1))
        else:
            html.append("prev")

        # page number links
        if inclPageLinks:
            for p in xrange(1, numPages+1):
                if p != pageNum:
                    html.append("<a href=\"dir%d.html\">%d</a>" % (p, p))
                else:
                    html.append("[%d]" % p)

        # next link
        if pageNum < numPages:
            html.append("<a href=\"dir%d.html\">next</a>" % (pageNum+1))
        else:
            html.append("next")
        return ", ".join(html)

    def __padRows(self, pg, numPadRows, numColumns):
        if numColumns > 1:
            pr = "<tr colspan=\"" + numColumns + "\"></tr>"
        else:
            pr = "<tr></tr>"
        for i in xrange(numPadRows):
            pg.add(pr)

    def __addPageRows(self, pg, pgEntries, numPadRows):
        """add one set of rows to the page.  In multi-column mode, this
        will be contained in a higher-level table"""
        pg.tableStart()
        if self.colNames != None:
            pg.tableHeader(self.colNames)
        numColumns = None
        for ent in pgEntries:
            numColumns = ent.numColumns()  # better all be the same
            pg.add(ent.toHtmlRow())
        if numPadRows > 0:
            self.__padRows(pg, numPadRows, numColumns)
        pg.tableEnd()

    def __addMultiColEntryTbl(self, pg, pgEntries):
        pg.tableStart()
        nEnts = len(pgEntries)
        rowsPerCol = nEnts/self.numColumns
        iEnt = 0
        pg.add("<tr>")
        for icol in xrange(self.numColumns):
            pg.add("<td>")
            if iEnt < nEnts-rowsPerCol:
                n = rowsPerCol
                np = 0
            else:
                n = nEnts-iEnt
                np = rowsPerCol - n
            self.__addPageRows(pg, pgEntries[iEnt:iEnt+n], np)
            pg.add("</td>")
        pg.add("</tr>")
        pg.tableEnd()
        

    def __addEntryTbl(self, pg, pgEntries):
        if self.numColumns > 1:
            self.__addMultiColEntryTbl(pg, pgEntries)
        else:
            self.__addPageRows(pg, pgEntries, 0)

    def __writeDirPage(self, outDir, pgEntries, pageNum, numPages):
        title = "page %d" % pageNum
        if self.title:
            title += ": " + self.title
        pg = HtmlPage(title=title, inStyle=self.style)
        pg.h3(title)
        if self.pageDesc != None:
            pg.add(self.pageDesc)
            pg.add("<br><br>")
        pg.add(self.__getPageLinks(pageNum, numPages, False))
        self.__addEntryTbl(pg, pgEntries)
        pg.add(self.__getPageLinks(pageNum, numPages, True))

        dirFile = outDir + "/dir%d.html" % pageNum
        pg.writeFile(dirFile)

    def __writeDirPages(self, outDir):
        if len(self.entries) == 0:
            # at least write an empty page
            self.__writeDirPage(outDir, [], 1, 0)
        elif self.pageSize == None:
            # single page
            self.__writeDirPage(outDir, self.entries, 1, 1)
        else:
            # split
            numPages = (len(self.entries)+self.pageSize-1)/self.pageSize
            for pageNum in xrange(1,numPages+1):
                first = (pageNum-1) * self.pageSize
                last = first+(self.pageSize-1)
                pgEntries = self.entries[first:last]
                self.__writeDirPage(outDir, pgEntries, pageNum, numPages)

    def write(self, outDir):
        fileOps.ensureDir(outDir)
        frame = self.__mkFrame(self.title, self.dirPercent, self.below)
        frame.writeFile(outDir + "/index.html")
        self.__writeDirPages(outDir)
