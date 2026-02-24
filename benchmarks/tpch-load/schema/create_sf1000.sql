CREATE SCHEMA IF NOT EXISTS load_tpch_sf1000_parquet WITH (LOCATION = concat('s3a://presto-workload-testing-95ced98/native_load_tpch_sf1000_parquet_',
                 format_datetime(current_timestamp, 'yyyyMMdd_HHmmss'), '/'));
USE load_tpch_sf1000_parquet;
