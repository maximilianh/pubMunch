# processed files of the publications pipeline, needed for update mode, to load only unprocessed files
# for subsequent updates
CREATE TABLE pubsLoadedFile (
        fileName varchar(1024), 
        size bigint,
        timeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

