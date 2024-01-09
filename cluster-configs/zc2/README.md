# `zc2` cluster
custom (vCPU: 64, Memory: 712 GB) * 28

**System reserved:** 4 GB (712 GB * 0.05,
Minimum 4 GB, Maximum 4 GB)

**Allocated to the Docker container:** 712 GB - 4 GB = 708 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (708 GB) * 0.9 = 637 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (637 GB) * 0.8 = 510 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (510 GB) * 0.9 = 459 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (510 GB) * `[number of nodes]` (28) = 14280 GB
  * `query.max-memory` = `query.max-memory-per-node` (459 GB) * `[number of nodes]` (28) = 12852 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (637 GB) * 0.2 = 127 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (708 GB) * 1 = 708 GB
* `query.max-memory-per-node` = `system-memory-gb` (708 GB) * 0.95 = 673 GB
* `query-memory-gb` = `query.max-memory-per-node` = 673 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
