# `xsmall` cluster
r5.xlarge (vCPU: 4, Memory: 32 GB) * 2

**System reserved:** 4 GB (32 GB * 0.03 or 4 GB, whichever is bigger)

**Allocated to the Docker container:** 32 GB - 4 GB = 28 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (28 GB) * 0.9 = 25 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (25 GB) * 0.8 = 20 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (20 GB) * 0.95 = 19 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (20 GB) * `[number of nodes]` (2) = 40 GB
  * `query.max-memory` = `query.max-memory-per-node` (19 GB) * `[number of nodes]` (2) = 38 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (25 GB) * 0.2 = 5 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (28 GB) * 0.9 = 25 GB
* `query.max-memory-per-node` = `system-memory-gb` (25 GB) * 1 = 25 GB
* `query-memory-gb` = `query.max-memory-per-node` = 25 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
