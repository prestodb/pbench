create table if not exists pbench_clusters
(
    cluster_name varchar(255) not null
        primary key,
    cluster_fqdn varchar(255) not null,
    created      datetime(3)  not null
);

-- create index pbench_clusters_cluster_fqdn_index on presto_benchmarks.pbench_clusters (cluster_fqdn);
