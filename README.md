# PBench

PBench is a toolkit for Presto and Trino performance testing and operations. Originally a replacement for [Benchto](https://github.com/prestodb/benchto), PBench has grown to include:
* **Benchmarking** — run configurable, concurrent query workloads with results capturing and correctness verification
* **Workload replay** — replay recorded production query traffic against a cluster
* **Query forwarding** — monitor a source cluster and forward queries to one or more target clusters in real time
* **Table schema saving** — export table metadata and statistics for reproducing schemas across environments
* **Result comparison** — diff query output directories to detect regressions
* **DDL generation** — generate CREATE/INSERT scripts for benchmark datasets (TPC-DS, TPC-H, etc.)
* **Query JSON analysis** — load query info JSON files into databases for offline analysis

For a detailed explanation of PBench's design and architecture, see [Comprehensive Performance Benchmarking, Monitoring, and Reporting Infrastructure for Presto and Prestissimo](https://github.com/prestodb/pbench/wiki/ComprehensivePerformanceBenchmarking.pdf).

### Getting Started

* [QuickStart](https://github.com/prestodb/pbench/wiki/QuickStart) — install and run your first benchmark in minutes
* [Installing PBench](https://github.com/prestodb/pbench/wiki/Installing-PBench) — platform-specific download and setup

### Writing Benchmarks

* [Configuring PBench](https://github.com/prestodb/pbench/wiki/Configuring-PBench) — create stage JSON files to define queries, sessions, and execution order
  * [Parameters](https://github.com/prestodb/pbench/wiki/Parameters) — complete reference for all stage JSON parameters

### Running Benchmarks

* [The Run Command](https://github.com/prestodb/pbench/wiki/The-Run-Command) — flags and options for `pbench run`
  * [Understanding PBench Output](https://github.com/prestodb/pbench/wiki/Understanding-PBench-Output) — output directory structure and result files
  * [Comparing Results](https://github.com/prestodb/pbench/wiki/Comparing-Benchmarks) — use `pbench cmp` to diff query output directories

### All Commands

* [Command Reference](https://github.com/prestodb/pbench/wiki/Command-Reference) — online help for every `pbench` subcommand (`run`, `cmp`, `forward`, `replay`, `save`, etc.)
* [Generating Benchmark Configurations](https://github.com/prestodb/pbench/wiki/Generating-Benchmark-Configurations) — use `pbench genconfig` to generate cluster configs from templates

### Contributing

* [Development](https://github.com/prestodb/pbench/wiki/Development) — building, testing, and contributing to PBench
