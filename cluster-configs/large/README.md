# `large` cluster
r5.8xlarge (vCPU: 32, Memory: 256 GB) * 16

* Global
  * `SysReservedGb = 4`
  * `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 256 - ceil(4) = 252` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* For Java clusters:
  * `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(252 * 0.9) = 226` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
  * [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
    * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(226 * 0.2) = 46`
    * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(226 * 0.8) = 180`
    * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(180 * 0.9) = 162`
    * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 180 * 16 = 2880`
    * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 162 * 16 = 2592` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
* For Prestissimo clusters:
  * Coordinator heap setting same as Java cluster
  * `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 16, 2)) = 2`
  * `NonVeloxBufferMemGb = 2`
  * `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 252 - 2 - ceil(2) = 248`
  * `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(248 * 0.95) = 235`
