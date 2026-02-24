-- Creating part table with PARQUET format
CREATE TABLE part (
    partkey     BIGINT NOT NULL,
    name        VARCHAR(55) NOT NULL,
    mfgr        VARCHAR(25) NOT NULL,
    brand       VARCHAR(10) NOT NULL,
    type        VARCHAR(25) NOT NULL,
    size        BIGINT NOT NULL,
    container   VARCHAR(10) NOT NULL,
    retailprice DECIMAL(12,2) NOT NULL,
    comment     VARCHAR(23) NOT NULL
) WITH (format = 'PARQUET');
