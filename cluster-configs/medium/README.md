# `medium` cluster
r5.4xlarge (vCPU: 16, Memory: 124 GB) * 8

### Global
* `SysReservedGb = 2`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 124 - ceil(2) = 122` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(122 * 0.9) = 109` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(109 * 0.2) = 22`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(109 * 0.8) = 87`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(87 * 0.9) = 78`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 87 * 8 = 696`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 78 * 8 = 624` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 8, 2)) = 1`
* `NonVeloxBufferMemGb = 8`
* `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 122 - 1 - ceil(8) = 113`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(113 * 0.95) = 107`
