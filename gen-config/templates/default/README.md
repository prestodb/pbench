# `{{ .Name }}` cluster
{{ .WorkerInstanceType }} (vCPU: {{ .VCPUPerWorker }}, Memory: {{ .MemoryPerNodeGb }} GB) * {{ .NumberOfWorkers }}

* Global
  * `SysReservedGb = {{ .GeneratorParameters.SysReservedGb }}`
  * `ContainerMemoryGb = MemoryPerNodeGb - ceil(SysReservedGb) = {{ .MemoryPerNodeGb }} - ceil({{ .GeneratorParameters.SysReservedGb }}) = {{ .ContainerMemoryGb }}` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* For Java clusters:
  * `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor({{ .ContainerMemoryGb }} * {{ .GeneratorParameters.HeapSizePercentOfContainerMem }}) = {{ .HeapSizeGb }}` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
  * [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
    * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil({{ .HeapSizeGb }} * {{ .GeneratorParameters.HeadroomPercentOfHeap }}) = {{ .HeadroomGb }}`
    * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor({{ .HeapSizeGb }} * {{ .GeneratorParameters.QueryMaxTotalMemPerNodePercentOfHeap }}) = {{ .JavaQueryMaxTotalMemPerNodeGb }}`
    * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor({{ .JavaQueryMaxTotalMemPerNodeGb }} * {{ .GeneratorParameters.QueryMaxMemPerNodePercentOfTotal }}) = {{ .JavaQueryMaxMemPerNodeGb }}`
    * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = {{ .JavaQueryMaxTotalMemPerNodeGb }} * {{ .NumberOfWorkers }} = {{ mul .JavaQueryMaxTotalMemPerNodeGb .NumberOfWorkers }}`
    * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = {{ .JavaQueryMaxMemPerNodeGb }} * {{ .NumberOfWorkers }} = {{ mul .JavaQueryMaxMemPerNodeGb .NumberOfWorkers }}` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
* For Prestissimo clusters:
  * Coordinator heap setting same as Java cluster
  * `NativeProxygenMemGb = ceil(min(ProxygenMemPerWorkerGb * NumberOfWorkers, ProxygenMemCapGb)) = ceil(min({{ .GeneratorParameters.ProxygenMemPerWorkerGb }} * {{ .NumberOfWorkers }}, {{ .GeneratorParameters.ProxygenMemCapGb }})) = {{ .NativeProxygenMemGb }}`
  * `NonVeloxBufferMemGb = {{ .GeneratorParameters.NonVeloxBufferMemGb }}`
  * `system-memory-gb = ContainerMemory - NativeProxygenMemGb - ceil(NonVeloxBufferMemGb) = {{ .ContainerMemoryGb }} - {{ .NativeProxygenMemGb }} - ceil({{ .GeneratorParameters.NonVeloxBufferMemGb }}) = {{ .NativeSystemMemGb }}`
  * `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor({{ .NativeSystemMemGb }} * {{ .GeneratorParameters.NativeQueryMemPercentOfSysMem }}) = {{ .NativeQueryMemGb }}`
