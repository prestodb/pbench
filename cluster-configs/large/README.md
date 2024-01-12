# `large` cluster
r5.8xlarge (vCPU: 32, Memory: 248 GB) * 16

* Global
  * `SysReservedGb = 1`
  * `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 248 - ceil(1) = 247` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* For Java clusters:
  * `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(247 * 0.9) = 222` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
  * [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
    * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(222 * 0.2) = 45`
    * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(222 * 0.8) = 177`
    * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(177 * 0.9) = 159`
    * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 177 * 16 = 2832`
    * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 159 * 16 = 2544` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
* For Prestissimo clusters:
  * Coordinator heap setting same as Java cluster
  * `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 16, 2)) = 2`
  * `NonVeloxBufferMemGb = 2`
  * `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 247 - 2 - ceil(2) = 243`
  * `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(243 * 0.95) = 230`
