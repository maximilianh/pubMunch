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

* Lippincott Williams = Wolters Kluwer

# not using this anymore, see below
#wget http://www.lww.com/opencms/opencms/web/PEMR/PDFs/docs/ratesheet.pdf
#pdftotext ratesheet.pdf
#iconv -f utf8 -t ascii -c ratesheet.txt | egrep '^[0-9]{8}$' | gawk 'BEGIN {print "pIssn"} // {print substr($1,0,4)"-"substr($1,4,4)}'  > lww.tab
#rm -f ratesheet.pdf ratesheet.txt

got lww.biborosch.tab from "Biborosch, Richard" <Rich.Biborosch@wolterskluwer.com> by email

echo -e 'title\turls\tpIssn\teIssn' | cat - lww.biborosch.txt | grep -v ^Title | cut -f1,2,3,4 | grep ^$'\t' -v > lww.tab

* Karger
wget http://misc.karger.com/Services/pdf/PL2013_USD.pdf 
pdftotext PL2013_USD.txt -enc ASCII7
echo eIssn `cat PL2013_USD.txt | egrep 'e-ISSN' | cut -d' ' -f2` | tr ' ' '\n' > karger.tab

* Scopus
Got title list from
http://www.elsevier.com/online-tools/scopus/content-overview

Also wrote asjc_codes.txt, from one of the other sheets in this file

has newlines in header fields, so needs very special converter

cd scopus
in2csv title_list.xlsx  -d '\t' -q '' > title_list.csv
csvconvert -i title_list.csv --dlm-output='	' --remove-line-char -o title_list.tab
mac2unix *.tab *.txt
python convert_scopus.py > ../scopus.tab

* CrossRef

curl http://ftp.crossref.org/titlelist/titleFile.csv | csvToTab /dev/stdin > crossref.tab

* Taylor and Francis

Got title list from http://www.tandfonline.com/page/title-lists
"Current Content Access" -> tandf.txt

cat tandf.txt | egrep -v ^publication_title | tawk 'BEGIN {print "eIssn\turls"; } {if ($3=="") {next;}; FS="\t"; OFS="\t"; print $3, "tandfonline.com"}' > tandf.tab
