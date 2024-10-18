# `xlarge-ssd` cluster
r6id.16xlarge (vCPU: 64, Memory: 490 GB) * 16

### This configuration includes an SSD Cache of 1800GB
### Global
* `SysReservedMemCapGb = 2`
* `SysReservedMemPercent = 0.05`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(min(SysReservedMemCapGb, MemoryPerNodeGb * SysReservedMemPercent)) = 490 - ceil(min(2, 490 * 0.05)) = 488` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* `JoinMaxBcastSizePercentOfContainerMem = 0.01`
* `JoinMaxBroadcastTableSizeMb = ceil(ContainerMemoryGb * JoinMaxBcastSizePercentOfContainerMem * 1024) = ceil(488 * 0.01 * 1024) = 4998MB`
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(488 * 0.9) = 439` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(439 * 0.2) = 88`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(439 * 0.8) = 351`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(351 * 0.9) = 315`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 351 * 16 = 5616`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 315 * 16 = 5040` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeBufferMemCapGb = 32`
* `NativeBufferMemPercent = 0.05`
* `NativeBufferMemGb = ceil(min(NativeBufferMemCapGb, ContainerMemoryGb * NativeBufferMemPercent)) = ceil(min(32, 488 * 0.05)) = 25`
* `NativeProxygenMemGb = ceil(min(ProxygenMemCapGb, ProxygenMemPerWorkerGb * NumberOfWorkers)) = ceil(min(2, 0.125 * 16)) = 2`

* `system-memory-gb = ContainerMemory - NativeBufferMemGb - NativeProxygenMemGb = 488 - 25 - 2 = 461`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(461 * 0.95) = 437`
### For Spark clusters: TODO
