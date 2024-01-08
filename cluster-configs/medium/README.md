# `medium` cluster
r5.4xlarge (vCPU: 16, Memory: 128 GB) * 8

**System reserved:** 6 GB (128 GB * 0.05 or 4 GB, whichever is bigger)

**Allocated to the Docker container:** 128 GB - 6 GB = 122 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (122 GB) * 0.9 = 110 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (110 GB) * 0.8 = 88 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (88 GB) * 0.9 = 79 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (88 GB) * `[number of nodes]` (8) = 704 GB
  * `query.max-memory` = `query.max-memory-per-node` (79 GB) * `[number of nodes]` (8) = 632 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (110 GB) * 0.2 = 22 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (122 GB) * 0.9 = 110 GB
* `query.max-memory-per-node` = `system-memory-gb` (110 GB) * 1 = 110 GB
* `query-memory-gb` = `query.max-memory-per-node` = 110 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
