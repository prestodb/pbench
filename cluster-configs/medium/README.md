# `medium` cluster
r5.4xlarge (vCPU: 16, Memory: 128 GB) * 8

* Global
  * `SysReservedGb = 4`
  * `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 128 - ceil(4) = 124` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* For Java clusters:
  * `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(124 * 0.9) = 111` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
  * [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
    * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(111 * 0.2) = 23`
    * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(111 * 0.8) = 88`
    * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(88 * 0.9) = 79`
    * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 88 * 8 = 704`
    * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 79 * 8 = 632` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
* For Prestissimo clusters:
  * Coordinator heap setting same as Java cluster
  * `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 8, 2)) = 1`
  * `NonVeloxBufferMemGb = 2`
  * `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 124 - 1 - ceil(2) = 121`
  * `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(121 * 0.95) = 114`
