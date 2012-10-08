# table with already processed article Ids, for Elsevier JSON export, to distinguish not found / not processed
CREATE TABLE processedArticles (
        articleId bigint, 
        externalId varchar(255),
        doi varchar(255),
        PRIMARY KEY (articleId),
        KEY `extIdx`(externalId),
        KEY `doiIdx`(doi)
        );

