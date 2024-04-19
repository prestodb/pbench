# `{{ .Name }}` cluster
{{ .WorkerInstanceType }} (vCPU: {{ .VCPUPerWorker }}, Memory: {{ .MemoryPerNodeGb }} GB) * {{ .NumberOfWorkers }}
{{ if .SpillEnabled }}
### This configuration include Prestissimo spilling settings.
{{ end }}
{{ if .SsdCacheSize }}
### This configuration includes an SSD Cache of {{ .SsdCacheSize }}GB
{{ end }}
### Global
* `SysReservedMemCapGb = {{ .GeneratorParameters.SysReservedMemCapGb }}`
* `SysReservedMemPercent = {{ .GeneratorParameters.SysReservedMemPercent }}`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(min(SysReservedMemCapGb, MemoryPerNodeGb * SysReservedMemPercent)) = {{ .MemoryPerNodeGb }} - ceil(min({{ .GeneratorParameters.SysReservedMemCapGb }}, {{ .MemoryPerNodeGb }} * {{ .GeneratorParameters.SysReservedMemPercent }})) = {{ .ContainerMemoryGb }}` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* `JoinMaxBcastSizePercentOfContainerMem = {{ .GeneratorParameters.JoinMaxBcastSizePercentOfContainerMem }}`
* `JoinMaxBroadcastTableSizeMb = ceil(ContainerMemoryGb * JoinMaxBcastSizePercentOfContainerMem * 1024) = ceil({{ .ContainerMemoryGb }} * {{ .GeneratorParameters.JoinMaxBcastSizePercentOfContainerMem }} * 1024) = {{ .JoinMaxBroadcastTableSizeMb }}MB`
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor({{ .ContainerMemoryGb }} * {{ .GeneratorParameters.HeapSizePercentOfContainerMem }}) = {{ .HeapSizeGb }}` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil({{ .HeapSizeGb }} * {{ .GeneratorParameters.HeadroomPercentOfHeap }}) = {{ .HeadroomGb }}`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor({{ .HeapSizeGb }} * {{ .GeneratorParameters.QueryMaxTotalMemPerNodePercentOfHeap }}) = {{ .JavaQueryMaxTotalMemPerNodeGb }}`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor({{ .JavaQueryMaxTotalMemPerNodeGb }} * {{ .GeneratorParameters.QueryMaxMemPerNodePercentOfTotal }}) = {{ .JavaQueryMaxMemPerNodeGb }}`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = {{ .JavaQueryMaxTotalMemPerNodeGb }} * {{ .NumberOfWorkers }} = {{ mul .JavaQueryMaxTotalMemPerNodeGb .NumberOfWorkers }}`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = {{ .JavaQueryMaxMemPerNodeGb }} * {{ .NumberOfWorkers }} = {{ mul .JavaQueryMaxMemPerNodeGb .NumberOfWorkers }}` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeBufferMemCapGb = {{ .GeneratorParameters.NativeBufferMemCapGb }}`
* `NativeBufferMemPercent = {{ .GeneratorParameters.NativeBufferMemPercent }}`
* `NativeBufferMemGb = ceil(min(NativeBufferMemCapGb, ContainerMemoryGb * NativeBufferMemPercent)) = ceil(min({{ .GeneratorParameters.NativeBufferMemCapGb }}, {{ .ContainerMemoryGb }} * {{ .GeneratorParameters.NativeBufferMemPercent }})) = {{ .NativeBufferMemGb }}`
* `NativeProxygenMemGb = ceil(min(ProxygenMemCapGb, ProxygenMemPerWorkerGb * NumberOfWorkers)) = ceil(min({{ .GeneratorParameters.ProxygenMemCapGb }}, {{ .GeneratorParameters.ProxygenMemPerWorkerGb }} * {{ .NumberOfWorkers }})) = {{ .NativeProxygenMemGb }}`

* `system-memory-gb = ContainerMemory - NativeBufferMemGb - NativeProxygenMemGb = {{ .ContainerMemoryGb }} - {{ .NativeBufferMemGb }} - {{ .NativeProxygenMemGb }} = {{ .NativeSystemMemGb }}`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor({{ .NativeSystemMemGb }} * {{ .GeneratorParameters.NativeQueryMemPercentOfSysMem }}) = {{ .NativeQueryMemGb }}`
