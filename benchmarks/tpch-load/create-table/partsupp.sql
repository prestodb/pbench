-- Creating partsupp table with PARQUET format
CREATE TABLE partsupp (
    partkey    BIGINT NOT NULL,
    suppkey    BIGINT NOT NULL,
    availqty   BIGINT NOT NULL,
    supplycost DECIMAL(12,2) NOT NULL,
    comment    VARCHAR(199) NOT NULL
) WITH (format = 'PARQUET');
