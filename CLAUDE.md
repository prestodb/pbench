# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Build for current platform
make

# Build for all platforms (darwin/linux, amd64/arm64)
make all

# Build with InfluxDB support
make TAGS=influx

# Build with experimental commands (round)
make TAGS=experimental

# Install locally (creates symlink to /usr/local/bin/pbench)
make install

# Generate cluster configurations from templates
make clusters
```

## Testing

```bash
# Run all tests
go test ./...

# Run a specific package's tests
go test ./stage
go test ./cmd/cmp
go test ./prestoapi

# Run a single test
go test ./stage -run TestConcurrentQueries
```

## Architecture

PBench is a Presto/Trino benchmark runner built with Cobra CLI. It replaces Benchto with support for concurrent workloads, result capture, and query log collection.

### Package Structure

- **main.go** - Entry point, calls `cmd.Execute()`
- **cmd/** - Cobra command definitions. Each subcommand has a wrapper file (e.g., `run.go`) and implementation package (e.g., `cmd/run/`)
- **prestoapi/** - Presto/Trino query helpers, unmarshaller, and column stats types (uses `presto-go` library)
- **stage/** - Core benchmark execution engine. A `Stage` defines queries, settings, and can chain to other stages via `next` field in JSON
- **utils/** - Shared utilities including Presto flag handling, ORM-style row extraction, and path helpers
- **clusters/** - Cluster configuration templates and generated configs
- **benchmarks/** - Benchmark definitions (TPC-DS, TPC-H, ClickBench, etc.) as JSON stage files and SQL queries

### Key Concepts

**Stages**: Benchmarks are defined as JSON files that specify queries (inline or via files), session parameters, catalog/schema, and execution settings. Stages form a DAG via `next` field, enabling sequential/parallel execution patterns. Each stage gets its own `*Stage` struct instance; mutable fields are per-instance. `SharedStageStates` is shared across all stages via thread-safe types.

**Stage Settings** (inherited by child stages unless overridden):
- `catalog`, `schema`, `timezone` - Presto session settings
- `cold_runs`, `warm_runs` - Number of runs per query
- `save_output`, `save_json`, `save_column_metadata` - Output capture options
- `abort_on_error` - Stop on first failure
- `random_execution`, `randomly_execute_until` - Random query selection mode

**Run Recorders**: Results can be recorded to file (default), InfluxDB (requires `TAGS=influx` build), or MySQL. The Pulumi recorder (`pulumi_run_recorder.go`) is internal-only with IBM PrestoDB infrastructure patterns.

**Error Handling in Stages**: `run()` return value is discarded; errors propagate via `result.QueryError`, logging, context cancellation, and exit code. `runShellScripts` only returns errors when `abort_on_error=true` or `ctx.Err()!=nil`; otherwise errors are logged but swallowed. Post-query and post-stage script errors are combined with `errors.Join`.

### Build Tags

- `influx` - Enables InfluxDB run recorder support. Without this tag, `stage/no_influx.go` provides a stub that returns an error if InfluxDB config is provided.
- `experimental` - Enables the `round` command for rounding decimal values in benchmark output files.

### Dependencies

- **Go 1.25.7+** - Required by `go.mod`
- **Python 3** - Required by some stage test hooks (shell scripts that invoke Python for JSON processing)

## Commands

- `run` - Execute benchmarks from stage JSON files
- `cmp` - Compare query results between two directories
- `genconfig` - Generate cluster configs from templates
- `genddl` - Generate DDL scripts
- `loadjson` - Load query JSON files into databases
- `replay` - Replay workloads from CSV
- `forward` - Forward queries between Presto clusters
- `save` - Save table schema/data information
- `queryplan` - Query plan visualization utilities
- `round` - Round decimal values in benchmark output files (requires `experimental` build tag)

## Workflow

- After completing any code change, check whether CLAUDE.md or code comments need to be updated to reflect the change.
- After completing a task, check if the [pbench.wiki](https://github.com/ethanyzhang/pbench/wiki) repo needs corresponding updates (e.g., new commands, changed behavior, updated usage).
- Before finishing, run `gofmt -w .`, `go vet ./...`, `staticcheck ./...`, and `go mod tidy` to ensure no formatting or lint issues remain.
- If cluster configs or templates change, run `make clusters` and commit the regenerated output.
