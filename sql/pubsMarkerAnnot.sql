#Text to Genome project, matches to markers (=identifiers of dbSNP, bands, genbank accessions, etc) in fulltext
CREATE TABLE pubsMarkerAnnot (
    articleId bigint not null,	# internal article ID, created during download
    fileId int not null,        # identifier of the file where the marker was found
    annotId int not null,	# unique identifier of this marker within a file
    fileDesc varchar(2000) not null, # description of file where sequence was found 
    fileUrl varchar(2000) not null, # url of file where sequence was found 
    #markerType enum('symbol', 'snp', 'band', 'gene'), # type of marker: snp, band or gene
    markerType varchar(255),    # type of marker: snp, band, gene, genbank, etc
    markerId varchar(255), # id of marker, e.g. TP53 or rs12354
    recogMarkerType varchar(255),    # type of marker: snp, band, gene, genbank, etc
    recogMarkerId varchar(255), # id of marker, e.g. TP53 or rs12354
    section enum('unknown', 'header', 'abstract', 'intro', 'methods', 'results', 'discussion', 'conclusions', 'ack', 'refs', 'supplement'), 
    snippet varchar(5000) not null,	# flanking text around marker match
        # Indices
    KEY articleIdx(articleId),
    KEY markerIdx1(markerType, markerId),
    KEY markerIdx2(recogMarkerType, recogMarkerId)
);
