# got 2mil adaptive biotech cdr3 sequences
# train a markov model on them
cat ~/public_html/adaptiveBiotech/googleAnnotate/human.tcrb.aa.tab | cut -f1 | tabToFasta stdin > humanCdr3.fa
faMarkov trainFa humanCdr3.fa cdr3.markov

# train a markov model on uniprot
cat /hive/data/outside/uniProtCurrent/uniprot_sprot.fasta | faMarkov trainFa stdin uniprot.markov 

# train a markov model on medline title names and abstracts
zcat /hive/data/inside/pubs/text/medline/*.articles.gz | cut -f17,18 | faMarkov trainText stdin medline.markov
