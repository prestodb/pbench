# `medium2` cluster
r5.4xlarge (vCPU: 16, Memory: 128 GB) * 16

### Global
* `SysReservedMemCapGb = 2`
* `SysReservedMemPercent = 0.05`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(min(SysReservedMemCapGb, MemoryPerNodeGb * SysReservedMemPercent)) = 128 - ceil(min(2, 128 * 0.05)) = 126` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(126 * 0.9) = 113` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(113 * 0.2) = 23`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(113 * 0.8) = 90`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(90 * 0.9) = 81`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 90 * 16 = 1440`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 81 * 16 = 1296` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeBufferMemCapGb = 16`
* `NativeBufferMemPercent = 0.1`
* `NativeBufferMemGb = ceil(min(NativeBufferMemCapGb, ContainerMemoryGb * NativeBufferMemPercent)) = ceil(min(16, 126 * 0.1)) = 13`
* `NativeProxygenMemGb = ceil(min(ProxygenMemCapGb, ProxygenMemPerWorkerGb * NumberOfWorkers)) = ceil(min(2, 0.125 * 16)) = 2`

* `system-memory-gb = ContainerMemory - NativeBufferMemGb - NativeProxygenMemGb = 126 - 13 - 2 = 111`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(111 * 0.95) = 105`
