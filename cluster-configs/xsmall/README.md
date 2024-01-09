# `xsmall` cluster
r5.xlarge (vCPU: 4, Memory: 32 GB) * 2

**System reserved:** 4 GB (32 GB * 0.05,
Minimum 4 GB, Maximum 4 GB)

**Allocated to the Docker container:** 32 GB - 4 GB = 28 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (28 GB) * 0.9 = 25 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (25 GB) * 0.8 = 20 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (20 GB) * 0.9 = 18 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (20 GB) * `[number of nodes]` (2) = 40 GB
  * `query.max-memory` = `query.max-memory-per-node` (18 GB) * `[number of nodes]` (2) = 36 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (25 GB) * 0.2 = 5 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (28 GB) * 1 = 28 GB
* `query.max-memory-per-node` = `system-memory-gb` (28 GB) * 0.95 = 27 GB
* `query-memory-gb` = `query.max-memory-per-node` = 27 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
