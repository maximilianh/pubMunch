These are the tools that I use for the UCSC Genocoding project, see
http://text.soe.ucsc.edu

Most start with the prefix "pub", the category and then the concrete
publisher. The categories are:

- pubPrepX = prepare directory structures. These are used to download
        taxon names, import gene models from websites like NCBI or
        UCSC. Not needed for 
- pubGetX = download files from publisher X (medline, pmc, elsevier)
- pubConvX = convert downloaded files to a pub format (tab-separated table
             ,fields defined in lib/pubStore.py)

More general tools are:

- pubRunAnnot = run an annotator from the scripts directory on text data in
             pub format
- pubRunMapReduce = run a map/reduce style job from "scripts" onto fulltext.
- pubCrawl = crawl papers from various publishers, needs a directory set up
             with pubPrepCrawlDir and the journalList directory
- pubLoad = load pub format files into mysql db
- pubMap = complex multi stage pipeline to find and map markers found in text 
           (sequences, snps, bands, genes, etc) to genomic locations 
           and create/load bed files into the ucsc browser

If you plan to use any of these, make sure to go over lib/pubConf.py first.
Most commands need some settings in the config file adapted to your particular
server / cluster system. E.g. pubCrawl needs your email address, pubConvX 
need the cluster system and various input/output directories.



BUGS to fix:

fixme: illegal DOI landing page
http://www.nature.com/doifinder/10.1046/j.1523-1747.1998.00092.x

URL constructor:
http://www.nature.com/nature/journal/v437/n7062/full/4371102a.html
for DOI  doi:10.1038/4371102a

URL construction for supplemental files:
http://www.nature.com/bjc/journal/v103/n10/suppinfo/6605908s1.html

no access page:
http://www.nature.com/nrclinonc/journal/v7/n11/full/nrclinonc.2010.119.html
- in wget, it triggers a 401 error



