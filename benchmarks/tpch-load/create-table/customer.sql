-- Creating customer table with PARQUET format
CREATE TABLE customer (
    custkey     BIGINT NOT NULL,
    name        VARCHAR(25) NOT NULL,
    address     VARCHAR(40) NOT NULL,
    nationkey   BIGINT NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    acctbal     DECIMAL(12,2) NOT NULL,
    mktsegment  VARCHAR(10) NOT NULL,
    comment     VARCHAR(117) NOT NULL
) WITH (format = 'PARQUET');