#!/bin/bash
# script for weekly updates of upstream databases for publications track, pdb and uniprot

set -o errexit                        # stop on errors

echo updating pdb
sh /hive/data/outside/pdb/sync.sh
echo updating uniprot
sh /hive/data/outside/uniProt/updateCurrent.sh
echo parsing uniprot
~/projects/pubs/tools/pubParseDb uniprot all
~/projects/pubs/tools/pubParseDb uniprotTrembl all
