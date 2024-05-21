set session iceberg.hive_statistics_merge_strategy='USE_NULLS_FRACTION_AND_NDV';
show stats for customer;
show stats for lineitem;
show stats for orders;
show stats for nation;
show stats for region;
show stats for part;
show stats for supplier;
show stats for partsupp;
