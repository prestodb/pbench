# `medium2` cluster
r5.4xlarge (vCPU: 16, Memory: 128 GB) * 16

* Global
  * `SysReservedGb = 1`
  * `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 128 - ceil(1) = 127` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* For Java clusters:
  * `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(127 * 0.9) = 114` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
  * [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
    * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(114 * 0.2) = 23`
    * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(114 * 0.8) = 91`
    * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(91 * 0.9) = 81`
    * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 91 * 16 = 1456`
    * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 81 * 16 = 1296` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
* For Prestissimo clusters:
  * Coordinator heap setting same as Java cluster
  * `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 16, 2)) = 2`
  * `NonVeloxBufferMemGb = 2`
  * `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 127 - 2 - ceil(2) = 123`
  * `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(123 * 0.95) = 116`
