-- Creating supplier table with PARQUET format
CREATE TABLE supplier (
    suppkey     BIGINT NOT NULL,
    name        VARCHAR(25) NOT NULL,
    address     VARCHAR(40) NOT NULL,
    nationkey   BIGINT NOT NULL,
    phone       VARCHAR(15) NOT NULL,
    acctbal     DECIMAL(12,2) NOT NULL,
    comment     VARCHAR(101) NOT NULL
) WITH (format = 'PARQUET');
