cut -f1 /hive/data/inside/pubs/map/{elsevier,pmc}/wordCounts.tab  | sort -u | tabToFasta -c stdin > commonWords.fa
# blat the common words against genbank
for i in /hive/data/outside/ncbi/nr/split/*.fa; do echo job.sh $i psl/`basename $i .fa`.psl; done > jobList
ssh ku
para create jobList
para push
exit
cat psl/*.psl > commonWords.psl

# remove BNC words
less commonWords.psl | cut -f22 | uniq | tr -d ',' | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/bnc/bnc.txt | tr [:lower:] [:upper:] > notWords.txt
# these strings are relatively certain to be not words

# now remove all non-BNC/Genbank word
lstOp remove commonWords.txt notWords.txt > commonFilteredWords.txt

# TRAIN MARKOV MODELS

# got 2mil adaptive biotech cdr3 sequences
# train a markov model on them
cat ~/public_html/adaptiveBiotech/googleAnnotate/human.tcrb.aa.tab | cut -f1 | tabToFasta stdin > humanCdr3.fa
faMarkov trainFa humanCdr3.fa cdr3.markov

# train a markov model on uniprot
cat /hive/data/outside/uniProtCurrent/uniprot_sprot.fasta | faMarkov trainFa stdin uniprot.markov 

# train a markov model on medline title names and abstracts
zcat /hive/data/inside/pubs/text/medline/*.articles.gz | cut -f17,18 | faMarkov trainText stdin medline.markov


