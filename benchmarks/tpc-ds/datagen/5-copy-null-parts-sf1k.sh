aws s3 mv --recursive s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/catalog_sales/data/cs_sold_date_sk=null/ s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/catalog_sales/data/cs_sold_date_sk=__HIVE_DEFAULT_PARTITION__/
aws s3 mv --recursive s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/store_sales/data/ss_sold_date_sk=null/ s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/store_sales/data/ss_sold_date_sk=__HIVE_DEFAULT_PARTITION__/
aws s3 mv --recursive s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/web_sales/data/ws_sold_date_sk=null/ s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/web_sales/data/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__/

# undo
aws s3 mv --recursive s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/catalog_sales/data/cs_sold_date_sk=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/catalog_sales/data/cs_sold_date_sk=null/
aws s3 mv --recursive s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/store_sales/data/ss_sold_date_sk=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/store_sales/data/ss_sold_date_sk=null/
aws s3 mv --recursive s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/web_sales/data/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__/ s3://presto-workload/tpcds-sf1000-parquet-iceberg-part/web_sales/data/ws_sold_date_sk=null/
