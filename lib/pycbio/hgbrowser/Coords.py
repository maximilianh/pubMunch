# Copyright 2006-2012 Mark Diekhans
"browser coordinates object"

from pycbio.sys.Immutable import Immutable

# FIXME: support MAF db.chrom syntax, single base syntax, etc.

class CoordsError(Exception):
    "Coordinate error"
    pass

class Coords(Immutable):
    """Browser coordinates
    Fields:
       chr, start, end
       db - optional genome database
    """

    def __parse__(self, coordsStr):
        "parse chr:start-end"
        try:
            self.chr, rng = str.split(coordsStr, ":")
            self.start, self.end = str.split(rng, "-")
            self.start = int(self.start)
            self.end = int(self.end)
        except Exception, e:
            raise CoordsError("invalid coordinates: \"" + str(coordsStr) + "\": " + str(e))

    def __init__(self, *args, **opts):
        """args are either one argument in the form chr:start-end, or
        chr, start, end. options are db="""
        Immutable.__init__(self)
        if len(args) == 1:
            self.__parse__(args[0])
        elif len(args) == 3:
            try:
                self.chr = args[0]
                self.start = int(args[1])
                self.end = int(args[2])
            except Exception, e:
                raise CoordsError("invalid coordinates: \"" + str(args) + "\": " + str(e))
        else:
            raise CoordsError("Coords() excepts either one or three arguments")
        self.db = opts.get("db")
        self.mkImmutable()

    def __str__(self):
        return self.chr + ":" + str(self.start) + "-" + str(self.end)

    def size(self):
        return self.end-self.start

    def overlaps(self, other):
        return ((self.chr == other.chr) and (self.start < other.end) and (self.end > other.start))

    def pad(self, frac=0.05, minBases=5):
        """return Coords, padded with a fraction of the range."""
        amt = int(frac*(self.end - self.start))
        if amt < minBases:
            amt = minBases
        st = self.start - amt
        if st < 0:
            st = 0
        return Coords(self.chr, st, self.end+amt)

    def __cmp__(self, other):
        if other == None:
            return -1
        d = cmp(self.db, other.db)
        if d == 0:
            d = cmp(self.chr, other.chr)
        if d == 0:
            d = cmp(self.start, other.start)
        if d == 0:
            d = cmp(self.end, other.end)
        return d

    def __hash__(self):
        return hash(self.db) + hash(self.chr) + hash(self.start)
