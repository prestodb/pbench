CREATE SCHEMA IF NOT EXISTS load_tpch_sf100_parquet WITH (LOCATION = 's3a://presto-workload/hive_native_load_tpch_sf100_parquet/');
--Do not need to explicitly speficy schema here
USE load_tpch_sf100_parquet;
