{{ template "prelude" . -}}
# `{{ .cluster_size }}` cluster
{{ .worker_instance_type }} (vCPU: {{ .vcpu_per_worker }}, Memory: {{ .memory_per_node_gb }} GB) * {{ .number_of_workers }}

{{ if .spill_enabled -}}
### This configuration include Prestissimo spilling settings.
{{ end -}}
{{ if .ssd_cache_size -}}
### This configuration includes an SSD Cache of {{ .ssd_cache_size }}GB
{{ end -}}
### Global
* `SysReservedMemCapGb = {{ .sys_reserved_mem_cap_gb }}`
* `SysReservedMemPercent = {{ .sys_reserved_mem_percent }}`
* `ContainerMemoryGb = MemoryPerNodeGb - ceil(min(SysReservedMemCapGb, MemoryPerNodeGb * SysReservedMemPercent)) = {{ .memory_per_node_gb }} - ceil(min({{ .sys_reserved_mem_cap_gb }}, {{ .memory_per_node_gb }} * {{ .sys_reserved_mem_percent }})) = {{ .container_memory_gb }}` [[docker-stack-java.yaml](docker-stack-java.yaml)] and [[docker-stack-native.yaml](docker-stack-native.yaml)]
* `JoinMaxBcastSizePercentOfContainerMem = {{ .join_max_bcast_size_percent_of_container_mem }}`
* `JoinMaxBroadcastTableSizeMb = ceil(ContainerMemoryGb * JoinMaxBcastSizePercentOfContainerMem * 1024) = ceil({{ .container_memory_gb }} * {{ .join_max_bcast_size_percent_of_container_mem }} * 1024) = {{ .join_max_broadcast_table_size_mb }}MB`
### For Java clusters:
* `HeapSizeGb = floor(ContainerMemory * HeapSizePercentOfContainerMem) = floor({{ .container_memory_gb }} * {{ .heap_size_percent_of_container_mem }}) = {{ .heap_size_gb }}` (`-Xmx` and `-Xms` in [[coordinator jvm.config](coordinator/jvm.config)] and [[worker jvm.config](workers/jvm.config)])
* [[coordinator config.properties](coordinator/config.properties)] and [[worker config.properties](worker/config.properties)]
  * `memory.heap-headroom-per-node = ceil(HeapSizeGb * HeadroomPercentOfHeap) = ceil({{ .heap_size_gb }} * {{ .headroom_percent_of_heap }}) = {{ .headroom_gb }}`
  * `query.max-total-memory-per-node = floor(HeapSizeGb * QueryMaxTotalMemPerNodePercentOfHeap) = floor({{ .heap_size_gb }} * {{ .query_max_total_mem_per_node_percent_of_heap }}) = {{ .java_query_max_total_mem_per_node_gb }}`
  * `query.max-memory-per-node = floor(query.max-total-memory-per-node * QueryMaxMemPerNodePercentOfTotal) = floor({{ .java_query_max_total_mem_per_node_gb }} * {{ .query_max_mem_per_node_percent_of_total }}) = {{ .java_query_max_mem_per_node_gb }}`
  * `query.max-total-memory = query.max-total-memory-per-node * NumberOfWorkers = {{ .java_query_max_total_mem_per_node_gb }} * {{ .number_of_workers }} = {{ mul .java_query_max_total_mem_per_node_gb .number_of_workers }}`
  * `query.max-memory = query.max-memory-per-node * NumberOfWorkers = {{ .java_query_max_mem_per_node_gb }} * {{ .number_of_workers }} = {{ mul .java_query_max_mem_per_node_gb .number_of_workers }}` [[documentation](https://prestodb.io/docs/current/admin/properties.html#memory-management-properties)]
### For Prestissimo clusters:
* Coordinator heap setting same as Java cluster
* `NativeBufferMemCapGb = {{ .native_buffer_mem_cap_gb }}`
* `NativeBufferMemPercent = {{ .native_buffer_mem_percent }}`
* `NativeBufferMemGb = ceil(min(NativeBufferMemCapGb, ContainerMemoryGb * NativeBufferMemPercent)) = ceil(min({{ .native_buffer_mem_cap_gb }}, {{ .container_memory_gb }} * {{ .native_buffer_mem_percent }})) = {{ .native_buffer_mem_gb }}`
* `NativeProxygenMemGb = ceil(min(ProxygenMemCapGb, ProxygenMemPerWorkerGb * NumberOfWorkers)) = ceil(min({{ .proxygen_mem_cap_gb }}, {{ .proxygen_mem_per_worker_gb }} * {{ .number_of_workers }})) = {{ .native_proxygen_mem_gb }}`

* `system-memory-gb = ContainerMemory - NativeBufferMemGb - NativeProxygenMemGb = {{ .container_memory_gb }} - {{ .native_buffer_mem_gb }} - {{ .native_proxygen_mem_gb }} = {{ .native_system_mem_gb }}`
* `query-memory-gb = query.max-memory-per-node = floor(system-memory-gb * NativeQueryMemPercentOfSysMem) = floor({{ .native_system_mem_gb }} * {{ .native_query_mem_percent_of_sys_mem }}) = {{ .native_query_mem_gb }}`
### For Spark clusters: TODO
