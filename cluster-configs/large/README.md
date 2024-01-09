# `large` cluster
r5.8xlarge (vCPU: 32, Memory: 256 GB) * 16

**System reserved:** 4 GB (256 GB * 0.05,
Minimum 4 GB, Maximum 4 GB)

**Allocated to the Docker container:** 256 GB - 4 GB = 252 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (252 GB) * 0.9 = 227 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (227 GB) * 0.8 = 182 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (182 GB) * 0.9 = 164 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (182 GB) * `[number of nodes]` (16) = 2912 GB
  * `query.max-memory` = `query.max-memory-per-node` (164 GB) * `[number of nodes]` (16) = 2624 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (227 GB) * 0.2 = 45 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (252 GB) * 1 = 252 GB
* `query.max-memory-per-node` = `system-memory-gb` (252 GB) * 0.95 = 239 GB
* `query-memory-gb` = `query.max-memory-per-node` = 239 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
