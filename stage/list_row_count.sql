select pbench_queries.query_file, pbench_queries.row_count
from pbench_queries
         join pbench_runs on pbench_runs.run_id = pbench_queries.run_id
where pbench_runs.run_name = '[REF] c1w0_java_oss_sf10k_ds_power_240312-093824'