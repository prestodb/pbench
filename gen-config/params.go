package gen_config

type GenerationParameters struct {
	SysReservedPercent float64 `json:"sys_reserved_percent"`
	MinSysReservedGb   float64 `json:"min_sys_reserved_gb"`
	// Container memory = (1 - SysReservedPercent) * Node memory
	HeapSizePercentOfContainerMem        float64 `json:"heap_size_percent_of_container_mem"`
	HeadroomPercentOfHeap                float64 `json:"headroom_percent_of_heap"`
	QueryMaxTotalMemPerNodePercentOfHeap float64 `json:"query_max_total_mem_per_node_percent_of_heap"`
	QueryMaxMemPerNodePercentOfTotal     float64 `json:"query_max_mem_per_node_percent_of_total"`
	NativeSysMemPercentOfContainerMem    float64 `json:"native_sys_mem_percent_of_container_mem"`
	NativeQueryMemPercentOfSysMem        float64 `json:"native_query_mem_percent_of_sys_mem"`
}

var DefaultGenerationParameters = &GenerationParameters{
	SysReservedPercent:                   0.03,
	MinSysReservedGb:                     4,
	HeapSizePercentOfContainerMem:        0.9,
	HeadroomPercentOfHeap:                0.2,
	QueryMaxTotalMemPerNodePercentOfHeap: 0.8,
	QueryMaxMemPerNodePercentOfTotal:     0.95,
	NativeSysMemPercentOfContainerMem:    0.9,
	NativeQueryMemPercentOfSysMem:        1,
}
