This directory contains lists of journals, their ISSNs and their publisher.
These data are needed to find out which ISSNs are published by a given 
publisher. The ISSNs are then used to query PubMed for the PMIDs linked
of an ISSN (the NLM has no notion of a publisher at all).

These lists are the input for pubPrepCrawlDir, it will parse them to tables.

If you want to update these lists:

* NLM Catalog:

Go to http://www.ncbi.nlm.nih.gov/nlmcatalog, click "Limits", select "only Pubmed journals" or
"currently indexed for Medline" (depending on your needs), click search, click "Send to", 
"File", format: "XML". You can gzip the file and replace the file here or provide the file name
to the pubPrepCrawl script.

* Highwire:

Goto http://highwire.stanford.edu/cgi/journalinfo, check
"Brief description of the journal"
"Who is the publisher"
"Where is the publisher's home page"
"What is the main URL of the journal site"
"What is the print ISSN number"

Click "select all journals"

Check "Check to also generate a tab-delimited file for importing into Microsoft Excel"

Enter your email address and wait until your receive an email.

Import the tab-sep file into Excel, replace the first row with
title	publisher	urls	eIssn	pIssn

save to a tab-sep file and replace the file highwire.tsv here (version from 2012)

* Wiley:

Download this excel file http://wileyonlinelibrary.com/journals-list and open
with Excel.

Use only the first sheet.

Create a new column "publisher" and fill it with "Wiley"

Rename print ISSN to pIssn
Rename electronic ISSN to eIssn
Rename Journal full title to title

Delete the other columns and all decorations

Save as tab-sep to wiley.tab

You can filter this to BioMed-articles with
cat wiley.tab | egrep 'Medical|Life|Agric|Chemistry|Environmental' > wileyFiltered.tab
