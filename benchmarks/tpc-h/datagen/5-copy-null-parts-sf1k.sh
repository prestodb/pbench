aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/lineitem/data/shipdate=null/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/lineitem/data/shipdate=__HIVE_DEFAULT_PARTITION__/
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/orders/data/orderdate=null/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/orders/data/orderdate=__HIVE_DEFAULT_PARTITION__/
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/customer/data/mktsegment=null/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/customer/data/mktsegment=__HIVE_DEFAULT_PARTITION__/
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/part/data/brand=null/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/part/data/brand=__HIVE_DEFAULT_PARTITION__/

# undo
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/lineitem/data/shipdate=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/lineitem/data/shipdate=null/
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/orders/data/orderdate=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/orders/data/orderdate=null/
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/customer/data/mktsegment=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/customer/data/mktsegment=null/
aws s3 mv --recursive s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/part/data/brand=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpch-sf1000-parquet-partitioned-iceberg/part/data/brand=null/
