#!/bin/bash
# script for daily updates of all pub data on UCSC's hgwdev

# SETUP
#set -o errexit                        # stop on errors
export PATH=$PATH:/cluster/bin/x86_64 # for hgLoadSqlTab

# VARIABLES
PYTHON=/cluster/software/bin/python
BIN=/hive/users/max/projects/pubs/tools/
DOWNBASE=/hive/data/outside/pubs
TEXTBASE=/hive/data/inside/pubs/text
BLATBASE=/hive/data/inside/pubs/blat

BLATDIR=${BLATBASE}/elsevier
CLUSTER=swarm

JOBBASE=/hive/data/inside/pubs/cronjob_runs
JOBDIR=$JOBBASE/`date +%m-%d-%y_%H:%M`
FLAGFILE=${JOBBASE}/cronjobRunning.flag

mkdir -p $JOBDIR
cd $JOBDIR

# always download, independent of flag file
# slightly dangerous, but elsevier and springer NEED
# to be downloaded, every day

echo JOB DIRECTORY $JOBDIR
echo DOWNLOAD 
echo __DOWNLOADING MEDLINE__
$PYTHON $BIN/pubGetMedline $DOWNBASE/medline
echo __DOWNLOADING ELSEVIER__
$PYTHON $BIN/pubGetElsevier $DOWNBASE/elsevier
echo __DOWNLOADING PUBMEDCENTRAL
$PYTHON $BIN/pubGetPmc $DOWNBASE/pmc
echo __DOWNLOADING SPRINGER
$PYTHON $BIN/pubGetSpringer $DOWNBASE/springer

# execute the rest only if there is no running job
if [ -e "${FLAGFILE}" ]
then 
   echo not running cronjob, ${FLAGFILE} exists, looks like an old one is still running.
   exit 1
fi

touch $FLAGFILE

echo
echo __CONVERT MEDLINE and update DB__ 
cd $JOBDIR; time $PYTHON $BIN/pubConvMedline --cluster=localhost $DOWNBASE/medline $TEXTBASE/medline
echo __CONVERT ELSEVIER___
cd $JOBDIR; $PYTHON $BIN/pubConvElsevier --cluster=localhost $DOWNBASE/elsevier $TEXTBASE/elsevier
echo __CONVERT PMC___
cd $JOBDIR; time $PYTHON $BIN/pubConvPmc --cluster=localhost $DOWNBASE/pmc $TEXTBASE/pmc
echo __CONVERT CRAWLER___
cd $JOBDIR; time $PYTHON $BIN/pubConvCrawler --cluster=localhost $DOWNBASE/crawler $TEXTBASE/crawler

echo
echo __CREATING CRAWLER REPORT
$PYTHON $BIN/pubCrawl $DOWNBASE/crawler --report /cluster/home/max/public_html/mining/crawlerStatus.html

rm -f $FLAGFILE

#echo BLAT AND LOAD INTO MYSQL
#ssh $CLUSTER "cd $JOBDIR; $PYTHON $BIN/pubBlat steps:all $BLATDIR -u notProcessed" && \
        #$PYTHON $BIN/pubBlat load $BLATDIR -u lastProcessed
