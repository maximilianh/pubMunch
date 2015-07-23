BIN=/hive/data/inside/pubs/bin
PYTHON=/cluster/software/bin/python2.7
VERSION=2
all:
	echo to copy the scripts to the production place on the hive
	echo run 'make install'

install:
	mkdir -p $(BIN)
	rm $(BIN)/*
	cp -R * $(BIN)/
	# most other hgwdev users don't have the correct python
	# in their path, so I have to force it
	sed -i 's|/usr/bin/env python|$(PYTHON)|' $(BIN)/pub*
	touch $(BIN)/changes_to_files_here_will_get_overwritten

tarball:
	cd .. && tar cvfz ~/public_html/pubtools/pubMunch_$(VERSION).tar.gz tools/*
	
# freezing on windows requires: python2.7 install as native windows installation
# (not cygwin) + native windows cxfreeze install
win:
	rm -rf build/exe.win-amd64-2.7/
	/cygdrive/c/Python27/python ucscScripts/setup.py build_exe

packWin:
	cd build/exe.win-amd64-2.7 && zip -r ../../pubToolsWin64.zip *
uploadWin:
	#scp pubToolsWin64.zip max@hgwdev.soe.ucsc.edu:public_html/pubtools/
	rsync pubToolsWin64.zip max@hgwdev.soe.ucsc.edu:public_html/pubtools/ --progress

bigFiles:
	tar cvfz bigFiles.tgz data/genes/* data/variants/* data/accessions/uniprot.sqlite
	mv bigFiles.tgz ~/public_html/pubs/tools/

data:
	curl http://hgwdev.soe.ucsc.edu/~max/pubs/tools/bigFiles.tgz | tar xvz
