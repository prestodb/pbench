# `large` cluster
r5.8xlarge (vCPU: 32, Memory: 256 GB) * 16

**System reserved:** 13 GB (256 GB * 0.05 or 4 GB, whichever is bigger)

**Allocated to the Docker container:** 256 GB - 13 GB = 243 GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` (243 GB) * 0.9 = 219 GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` (219 GB) * 0.8 = 175 GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` (175 GB) * 0.9 = 158 GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` (175 GB) * `[number of nodes]` (16) = 2800 GB
  * `query.max-memory` = `query.max-memory-per-node` (158 GB) * `[number of nodes]` (16) = 2528 GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` (219 GB) * 0.2 = 44 GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` (243 GB) * 0.9 = 219 GB
* `query.max-memory-per-node` = `system-memory-gb` (219 GB) * 1 = 219 GB
* `query-memory-gb` = `query.max-memory-per-node` = 219 GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
