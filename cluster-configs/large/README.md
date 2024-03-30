# `large` cluster
r5.8xlarge (vCPU: 32, Memory: 248 GB) * 16

### Global
* `SysReservedMemCapGb = 2`
* `SysReservedMemPercent = 0.05`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(min(SysReservedMemCapGb, MemoryPerNodeGb * SysReservedMemPercent)) = 248 - ceil(min(2, 248 * 0.05)) = 246` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* `JoinMaxBcastSizePercentOfContainerMem = 0.01`
* `JoinMaxBroadcastTableSizeMb = ceil(ContainerMemoryGb * JoinMaxBcastSizePercentOfContainerMem * 1024) = ceil(246 * 0.01 * 1024) = 2520MB`
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(246 * 0.9) = 221` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(221 * 0.2) = 45`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(221 * 0.8) = 176`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(176 * 0.9) = 158`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 176 * 16 = 2816`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 158 * 16 = 2528` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeBufferMemCapGb = 32`
* `NativeBufferMemPercent = 0.05`
* `NativeBufferMemGb = ceil(min(NativeBufferMemCapGb, ContainerMemoryGb * NativeBufferMemPercent)) = ceil(min(32, 246 * 0.05)) = 13`
* `NativeProxygenMemGb = ceil(min(ProxygenMemCapGb, ProxygenMemPerWorkerGb * NumberOfWorkers)) = ceil(min(2, 0.125 * 16)) = 2`

* `system-memory-gb = ContainerMemory - NativeBufferMemGb - NativeProxygenMemGb = 246 - 13 - 2 = 231`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(231 * 0.95) = 219`
