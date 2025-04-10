-- Creating orders table with PARQUET format
CREATE TABLE orders (
    orderkey       BIGINT NOT NULL,
    custkey        BIGINT NOT NULL,
    orderstatus    VARCHAR(1) NOT NULL,
    totalprice     DECIMAL(12,2) NOT NULL,
    orderdate      DATE NOT NULL,
    orderpriority  VARCHAR(15) NOT NULL,
    clerk          VARCHAR(15) NOT NULL,
    shippriority   BIGINT NOT NULL,
    comment        VARCHAR(79) NOT NULL
) WITH (format = 'PARQUET');