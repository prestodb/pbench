# `{{ .Name }}` cluster
{{ .WorkerInstanceType }} (vCPU: {{ .VCPUPerWorker }}, Memory: {{ .MemoryPerNodeGb }} GB) * {{ .NumberOfWorkers }}

**System reserved:** {{ .SystemReservedGb }} GB

**Allocated to the Docker container:** {{ .MemoryPerNodeGb }} GB - {{ .SystemReservedGb }} GB = {{ .ContainerMemoryGb }} GB [[docker-stack-java.yaml](docker-stack-java.yaml)] [[docker-stack-native.yaml](docker-stack-native.yaml)]

**For Java clusters:**
* `HeapSize` = `ContainerMemory` ({{ .ContainerMemoryGb }} GB) * {{ .GeneratorParameters.HeapSizePercentOfContainerMem }} = {{ .HeapSizeGb }} GB (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* Presto: [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `query.max-total-memory-per-node` = `HeapSize` ({{ .HeapSizeGb }} GB) * {{ .GeneratorParameters.QueryMaxTotalMemPerNodePercentOfHeap }} = {{ .JavaQueryMaxTotalMemPerNodeGb }} GB
  * `query.max-memory-per-node` = `query.max-total-memory-per-node` ({{ .JavaQueryMaxTotalMemPerNodeGb }} GB) * {{ .GeneratorParameters.QueryMaxMemPerNodePercentOfTotal }} = {{ .JavaQueryMaxMemPerNodeGb }} GB
  * `query.max-total-memory` = `query.max-total-memory-per-node` ({{ .JavaQueryMaxTotalMemPerNodeGb }} GB) * `[number of nodes]` ({{ .NumberOfWorkers }}) = {{ mul .JavaQueryMaxTotalMemPerNodeGb .NumberOfWorkers }} GB
  * `query.max-memory` = `query.max-memory-per-node` ({{ .JavaQueryMaxMemPerNodeGb }} GB) * `[number of nodes]` ({{ .NumberOfWorkers }}) = {{ mul .JavaQueryMaxMemPerNodeGb .NumberOfWorkers }} GB [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
  * `memory.heap-headroom-per-node` = `HeapSize` ({{ .HeapSizeGb }} GB) * {{ .GeneratorParameters.HeadroomPercentOfHeap }} = {{ .HeadroomGb }} GB

**For Prestissimo clusters:**
* Coordinator heap setting same as Java cluster
* `system-memory-gb` = `ContainerMemory` ({{ .ContainerMemoryGb }} GB) * {{ .GeneratorParameters.NativeSysMemPercentOfContainerMem }} = {{ .NativeSystemMemGb }} GB
* `query.max-memory-per-node` = `system-memory-gb` ({{ .NativeSystemMemGb }} GB) * {{ .GeneratorParameters.NativeQueryMemPercentOfSysMem }} = {{ .NativeQueryMemGb }} GB
* `query-memory-gb` = `query.max-memory-per-node` = {{ .NativeQueryMemGb }} GB
  * `MemoryForSpillingAndCaching` = `query.max-memory-per-node` - `query-memory-gb`. We don't need this for the benchmarking now so `query-memory-gb` = `query.max-memory-per-node`
