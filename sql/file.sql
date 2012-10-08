#file data table
CREATE TABLE file (
    fileId bigint not null,	# internal file ID, created during download
    articleId bigint not null,	# internal article ID, created during download
    url varchar(255) not null,	# URL to file
    description varchar(255),	# description of file
    fileType varchar(255),     # file extension, e.g. pdf
    time varchar(255),	# time when downloaded
    mimeType varchar(255),	# mimetype of file
    content longblob NOT NULL,	 # content of file, in utf8
    PRIMARY KEY(fileId),
    KEY articleIdx(articleId),
    KEY fileIdx(fileId)
)
DEFAULT CHARACTER SET 'utf8'
;
