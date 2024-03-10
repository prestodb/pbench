create table if not exists pbench_runs
(
    run_id       bigint auto_increment
        primary key,
    run_name     varchar(255) not null,
    cluster_fqdn varchar(255) not null,
    start_time   datetime(3)  not null,
    queries_ran  int          null,
    failed       int          null,
    mismatch     int          null,
    duration_ms  int          null,
    constraint pbench_runs_run_id
        unique (run_id)
);

-- create index pbench_runs_run_name_index on presto_benchmarks.pbench_runs (run_name);
