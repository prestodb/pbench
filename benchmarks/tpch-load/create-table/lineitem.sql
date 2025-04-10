-- Creating lineitem table with PARQUET format
CREATE TABLE lineitem (
    orderkey      BIGINT NOT NULL,
    partkey       BIGINT NOT NULL,
    suppkey       BIGINT NOT NULL,
    linenumber    BIGINT NOT NULL,
    quantity      DECIMAL(12,2) NOT NULL,
    extendedprice DECIMAL(12,2) NOT NULL,
    discount      DECIMAL(12,2) NOT NULL,
    tax           DECIMAL(12,2) NOT NULL,
    returnflag    VARCHAR(1) NOT NULL,
    linestatus    VARCHAR(1) NOT NULL,
    shipdate      DATE NOT NULL,
    commitdate    DATE NOT NULL,
    receiptdate   DATE NOT NULL,
    shipinstruct  VARCHAR(25) NOT NULL,
    shipmode      VARCHAR(10) NOT NULL,
    comment       VARCHAR(44) NOT NULL
) WITH (format = 'PARQUET');