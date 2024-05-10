CREATE TABLE IF NOT EXISTS
    query_events_raw (record varchar, dt varchar)
    WITH (external_location = '<data_location>', format = 'textfile', partitioned_by = ARRAY['dt']);

-- After creating external table, sync the partitions
CALL system.sync_partition_metadata('schema_name', 'query_events_raw', 'FULL');