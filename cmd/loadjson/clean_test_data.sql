DELETE FROM presto_query_creation_info WHERE query_id = '20240422_013209_00111_k6ve9_show_schema';
DELETE FROM presto_query_operator_stats WHERE query_id = '20240422_013209_00111_k6ve9_show_schema';
DELETE FROM presto_query_plans WHERE query_id = '20240422_013209_00111_k6ve9_show_schema';
DELETE FROM presto_query_stage_stats WHERE query_id = '20240422_013209_00111_k6ve9_show_schema';
DELETE FROM presto_query_statistics WHERE query_id = '20240422_013209_00111_k6ve9_show_schema';

DELETE FROM presto_query_creation_info WHERE query_id = '20240423_081548_00000_rrtm7_error';
DELETE FROM presto_query_operator_stats WHERE query_id = '20240423_081548_00000_rrtm7_error';
DELETE FROM presto_query_plans WHERE query_id = '20240423_081548_00000_rrtm7_error';
DELETE FROM presto_query_stage_stats WHERE query_id = '20240423_081548_00000_rrtm7_error';
DELETE FROM presto_query_statistics WHERE query_id = '20240423_081548_00000_rrtm7_error';

DELETE FROM presto_query_creation_info WHERE query_id = '20240427_073513_00020_5vbe8_q88';
DELETE FROM presto_query_operator_stats WHERE query_id = '20240427_073513_00020_5vbe8_q88';
DELETE FROM presto_query_plans WHERE query_id = '20240427_073513_00020_5vbe8_q88';
DELETE FROM presto_query_stage_stats WHERE query_id = '20240427_073513_00020_5vbe8_q88';
DELETE FROM presto_query_statistics WHERE query_id = '20240427_073513_00020_5vbe8_q88';

DELETE FROM presto_query_creation_info WHERE query_id LIKE 'json041724%';
DELETE FROM presto_query_operator_stats WHERE query_id LIKE 'json041724%';
DELETE FROM presto_query_plans WHERE query_id LIKE 'json041724%';
DELETE FROM presto_query_stage_stats WHERE query_id LIKE 'json041724%';
DELETE FROM presto_query_statistics WHERE query_id LIKE 'json041724%';

DELETE q, r
FROM pbench_queries q
         JOIN pbench_runs r ON q.run_id = r.run_id
WHERE r.run_name LIKE 'json041724-0426';
