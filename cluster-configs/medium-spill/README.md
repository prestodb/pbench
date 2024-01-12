# `medium-spill` cluster
r5.4xlarge (vCPU: 16, Memory: 124 GB) * 8

* Global
  * `SysReservedGb = 1`
  * `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = 124 - ceil(1) = 123` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* For Java clusters:
  * `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor(123 * 0.9) = 110` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
  * [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
    * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil(110 * 0.2) = 22`
    * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor(110 * 0.8) = 88`
    * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor(88 * 0.9) = 79`
    * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = 88 * 8 = 704`
    * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = 79 * 8 = 632` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
* For Prestissimo clusters:
  * Coordinator heap setting same as Java cluster
  * `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min(0.125 * 8, 2)) = 1`
  * `NonVeloxBufferMemGb = 2`
  * `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = 123 - 1 - ceil(2) = 120`
  * `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor(120 * 0.95) = 114`
