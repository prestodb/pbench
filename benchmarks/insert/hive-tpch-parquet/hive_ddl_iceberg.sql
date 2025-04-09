CREATE SCHEMA IF NOT EXISTS tpch_sf10_load_iceberg WITH (LOCATION = 's3a://presto-workload/tpch-sf10-load-iceberg/');
USE tpch_sf10_load_iceberg;

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

-- Creating partsupp table with PARQUET format
CREATE TABLE partsupp (
    partkey    BIGINT NOT NULL,
    suppkey    BIGINT NOT NULL,
    availqty   BIGINT NOT NULL,
    supplycost DECIMAL(12,2) NOT NULL,
    comment    VARCHAR(199) NOT NULL
) WITH (format = 'PARQUET');

-- Creating nation table with PARQUET format
CREATE TABLE nation (
    nationkey   BIGINT NOT NULL,
    name        VARCHAR(25) NOT NULL,
    regionkey   BIGINT NOT NULL,
    comment     VARCHAR(152) NOT NULL
) WITH (format = 'PARQUET');

-- Creating region table with PARQUET format
CREATE TABLE region (
    regionkey   BIGINT NOT NULL,
    name        VARCHAR(25) NOT NULL,
    comment     VARCHAR(152) NOT NULL
) WITH (format = 'PARQUET');