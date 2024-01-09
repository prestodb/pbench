# `medium` cluster
r5.4xlarge (vCPU: 16, Memory: 128 GB) * 8

**System reserved:** 4 GB (128 GB * 0.05,
Minimum 4 GB, Maximum 4 GB)

**Allocated to the Docker container:** 128 GB - 4 GB = 124 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (124 GB) * 0.9 = 112 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (112 GB) * 0.8 = 90 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (90 GB) * 0.9 = 81 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (90 GB) * `[number of nodes]` (8) = 720 GB
  * `query.max-memory` = `query.max-memory-per-node` (81 GB) * `[number of nodes]` (8) = 648 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (112 GB) * 0.2 = 22 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (124 GB) * 1 = 124 GB
* `query.max-memory-per-node` = `system-memory-gb` (124 GB) * 0.95 = 118 GB
* `query-memory-gb` = `query.max-memory-per-node` = 118 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
