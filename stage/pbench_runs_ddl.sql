create table if not exists pbench_runs
(
    run_id      bigint auto_increment,
    run_name    varchar(255) not null
        primary key,
    start_time  datetime(3)  not null,
    queries_ran int          null,
    duration_ms int          null,
    constraint pbench_runs_run_id
        unique (run_id)
);
