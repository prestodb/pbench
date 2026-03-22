delete q, r
from pbench_queries q
join pbench_runs r on q.run_id = r.run_id
where r.run_id IN (2834, 2832);

delete q, r, p1, p2, p3, p4, p5
from pbench_queries q
left join pbench_runs r on q.run_id = r.run_id
left join presto_query_creation_info p1 on q.query_id = p1.query_id
left join presto_query_operator_stats p2 on p1.query_id = p2.query_id
left join presto_query_plans p3 on p1.query_id = p3.query_id
left join presto_query_stage_stats p4 on p1.query_id = p4.query_id
left join presto_query_statistics p5 on p1.query_id = p5.query_id
where r.run_id between 3078 and 3083;

DELETE t FROM presto_benchmarks.presto_query_creation_info t
                  LEFT JOIN presto_benchmarks.pbench_queries p ON t.query_id = p.query_id
WHERE p.query_id IS NULL;

DELETE t FROM presto_benchmarks.presto_query_operator_stats t
                  LEFT JOIN presto_benchmarks.pbench_queries p ON t.query_id = p.query_id
WHERE p.query_id IS NULL;

DELETE t FROM presto_benchmarks.presto_query_plans t
                  LEFT JOIN presto_benchmarks.pbench_queries p ON t.query_id = p.query_id
WHERE p.query_id IS NULL;

DELETE t FROM presto_benchmarks.presto_query_stage_stats t
                  LEFT JOIN presto_benchmarks.pbench_queries p ON t.query_id = p.query_id
WHERE p.query_id IS NULL;

DELETE t FROM presto_benchmarks.presto_query_statistics t
                  LEFT JOIN presto_benchmarks.pbench_queries p ON t.query_id = p.query_id
WHERE p.query_id IS NULL;
