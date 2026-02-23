delete q, r
from pbench_queries q
join pbench_runs r on q.run_id = r.run_id
where r.run_id IN (2834, 2832);

delete q, r, p1, p2, p3, p4, p5
from pbench_queries q
join pbench_runs r on q.run_id = r.run_id
join presto_query_creation_info p1 on q.query_id = p1.query_id
join presto_query_operator_stats p2 on p1.query_id = p2.query_id
join presto_query_plans p3 on p1.query_id = p3.query_id
join presto_query_stage_stats p4 on p1.query_id = p4.query_id
join presto_query_statistics p5 on p1.query_id = p5.query_id
where r.run_id IN (2834, 2832);

delete p1, p2, p3, p4, p5
from pbench_queries q
         join pbench_runs r on q.run_id = r.run_id
         join presto_query_creation_info p1 on q.query_id = p1.query_id
         join presto_query_operator_stats p2 on p1.query_id = p2.query_id
         join presto_query_plans p3 on p1.query_id = p3.query_id
         join presto_query_stage_stats p4 on p1.query_id = p4.query_id
         join presto_query_statistics p5 on p1.query_id = p5.query_id
where r.run_id IN (2833);