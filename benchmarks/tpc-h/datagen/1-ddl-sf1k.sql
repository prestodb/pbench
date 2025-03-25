USE iceberg;
CREATE SCHEMA IF NOT EXISTS tpch_sf1000_parquet_partitioned_iceberg LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/';

USE iceberg.tpch_sf1000_parquet_partitioned_iceberg;

CREATE TABLE IF NOT EXISTS customer (
   custkey bigint,
   name varchar(25),
   address varchar(40),
   nationkey bigint,
   phone varchar(15),
   acctbal double,
   comment varchar(117),
   mktsegment varchar(10)
)
USING iceberg
PARTITIONED BY (mktsegment)
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/customer';

CREATE TABLE IF NOT EXISTS lineitem (
  orderkey bigint,
  partkey bigint,
  suppkey bigint,
  linenumber integer,
  quantity double,
  extendedprice double,
  discount double,
  tax double,
  returnflag varchar(1),
  linestatus varchar(1),
  commitdate date,
  receiptdate date,
  shipinstruct varchar(25),
  shipmode varchar(10),
  comment varchar(44),
  shipdate date
)
USING iceberg
PARTITIONED BY (shipdate)
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/lineitem';

CREATE TABLE IF NOT EXISTS orders (
  orderkey bigint,
  custkey bigint,
  orderstatus varchar(1),
  totalprice double,
  orderpriority varchar(15),
  clerk varchar(15),
  shippriority integer,
  comment varchar(79),
  orderdate date
)
USING iceberg
PARTITIONED BY (orderdate)
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/orders';

CREATE TABLE IF NOT EXISTS nation (
  nationkey bigint,
  name varchar(25),
  regionkey bigint,
  comment varchar(152))
USING iceberg
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/nation';

CREATE TABLE IF NOT EXISTS region (
  regionkey bigint,
  name varchar(25),
  comment varchar(152)
)
USING iceberg
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/region';

CREATE TABLE IF NOT EXISTS part (
  partkey bigint,
  name varchar(55),
  mfgr varchar(25),
  type varchar(25),
  size integer,
  container varchar(10),
  retailprice double,
  comment varchar(23),
  brand varchar(10)
)
USING iceberg
PARTITIONED BY (brand)
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/part';

CREATE TABLE IF NOT EXISTS supplier (
  suppkey bigint,
  name varchar(25),
  address varchar(40),
  nationkey bigint,
  phone varchar(15),
  acctbal double,
  comment varchar(101)
)
USING iceberg
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/supplier';

CREATE TABLE IF NOT EXISTS partsupp (
  partkey bigint,
  suppkey bigint,
  availqty integer,
  supplycost double,
  comment varchar(199)
)
USING iceberg
LOCATION 's3a://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/partsupp';
