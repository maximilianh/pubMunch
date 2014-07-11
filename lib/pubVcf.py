""" wrapper around pysam, parse tabix indexed vcf files

>>> vcf = VcfDir("/gbdb/hg19/1000Genomes")
>>> row, infoDict =  vcf.fetch("1", 0, 11000).next()
>>> row.ref, row.alt
('G', 'A')
>>> infoDict
{'AA': '.', 'AVGPOST': '0.7707', 'ERATE': '0.0161', 'AFR_AF': '0.04', 'LDAF': '0.2327', 'ASN_AF': '0.13', 'AF': '0.14', 'AMR_AF': '0.17', 'AC': '314', 'AN': '2184', 'EUR_AF': '0.21', 'VT': 'SNP', 'SNPSOURCE': 'LOWCOV', 'THETA': '0.0046', 'RSQ': '0.4319'}
"""

import pysam, glob
from os.path import join, basename

class VcfDir:
    def __init__(self, path):
        """ open tabixes, one per chrom """
        self.fhDict = {}
        for fname in glob.glob(join(path, "*.vcf.gz")):
            chrom = basename(fname).split(".")[1].replace("chr", "")
            self.fhDict[chrom] = pysam.Tabixfile(fname)
            #self.fnameDict[chrom] = fname

    def fetch(self, chrom, start, end):
        """ yield tuples (vcf object + info dict key->val) for a range 
        vcf row attributes are: 
        contig
        pos chromosomal position, zero-based
        id
        ref reference
        alt alt
        qual qual
        filter filter
        info info
        format format specifier.
        """
        chrom = chrom.replace("chr","")
        #fname = self.fnameDict[chrom]
        #vcf = pysam.VCF()
        #vcf.connect(fname)
        tbi = self.fhDict[chrom]
        it = tbi.fetch(chrom, start, end, parser=pysam.asVCF())
        for row in it:
            infoDict = {}
            infoStr = row.info
            for keyVal in infoStr.split(";"):
                if "=" not in keyVal:
                    continue
                key, val = keyVal.split("=")
                infoDict[key] = val
            yield row, infoDict


if __name__=="__main__":
    import doctest
    doctest.testmod()
