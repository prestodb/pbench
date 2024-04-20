# `xsmall` cluster
r5.xlarge (vCPU: 4, Memory: 30 GB) * 2

### Global
* `SysReservedMemCapGb = 2`
* `SysReservedMemPercent = 0.05`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(min(SysReservedMemCapGb, MemoryPerNodeGb * SysReservedMemPercent)) = 30 - ceil(min(2, 30 * 0.05)) = 28` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* `JoinMaxBcastSizePercentOfContainerMem = 0.01`
* `JoinMaxBroadcastTableSizeMb = ceil(ContainerMemoryGb * JoinMaxBcastSizePercentOfContainerMem * 1024) = ceil(28 * 0.01 * 1024) = 287MB`
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(28 * 0.9) = 25` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(25 * 0.2) = 5`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(25 * 0.8) = 20`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(20 * 0.9) = 18`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 20 * 2 = 40`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 18 * 2 = 36` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeBufferMemCapGb = 32`
* `NativeBufferMemPercent = 0.05`
* `NativeBufferMemGb = ceil(min(NativeBufferMemCapGb, ContainerMemoryGb * NativeBufferMemPercent)) = ceil(min(32, 28 * 0.05)) = 2`
* `NativeProxygenMemGb = ceil(min(ProxygenMemCapGb, ProxygenMemPerWorkerGb * NumberOfWorkers)) = ceil(min(2, 0.125 * 2)) = 1`

* `system-memory-gb = ContainerMemory - NativeBufferMemGb - NativeProxygenMemGb = 28 - 2 - 1 = 25`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(25 * 0.95) = 23`
