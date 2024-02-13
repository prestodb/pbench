# `xsmall` cluster
r5.xlarge (vCPU: 4, Memory: 30 GB) * 2

### Global
* `SysReservedGb = 2`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 30 - ceil(2) = 28` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
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
* `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 2, 2)) = 1`
* `NonVeloxBufferMemGb = 8`
* `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 28 - 1 - ceil(8) = 19`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(19 * 0.95) = 18`
