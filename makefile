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
	
