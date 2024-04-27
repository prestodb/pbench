package genconfig

import (
	_ "embed"
	"encoding/json"
)

type GeneratorParameters struct {
	// Container memory = node memory - SysReservedMemCapGb
	SysReservedMemCapGb                   float64 `json:"sys_reserved_mem_cap_gb"`
	SysReservedMemPercent                 float64 `json:"sys_reserved_mem_percent"`
	HeapSizePercentOfContainerMem         float64 `json:"heap_size_percent_of_container_mem"`
	HeadroomPercentOfHeap                 float64 `json:"headroom_percent_of_heap"`
	QueryMaxTotalMemPerNodePercentOfHeap  float64 `json:"query_max_total_mem_per_node_percent_of_heap"`
	QueryMaxMemPerNodePercentOfTotal      float64 `json:"query_max_mem_per_node_percent_of_total"`
	ProxygenMemPerWorkerGb                float64 `json:"proxygen_mem_per_worker_gb"`
	ProxygenMemCapGb                      float64 `json:"proxygen_mem_cap_gb"`
	NativeBufferMemPercent                float64 `json:"native_buffer_mem_percent"`
	NativeBufferMemCapGb                  float64 `json:"native_buffer_mem_cap_gb"`
	NativeQueryMemPercentOfSysMem         float64 `json:"native_query_mem_percent_of_sys_mem"`
	JoinMaxBcastSizePercentOfContainerMem float64 `json:"join_max_bcast_size_percent_of_container_mem"`
	MemoryPushBackStartBelowLimitGb       uint    `json:"memory_push_back_start_below_limit_gb"`
}

var (
	//go:embed params.json
	DefaultGeneratorParametersBytes []byte
	DefaultGeneratorParameters      = &GeneratorParameters{}
)

func init() {
	if err := json.Unmarshal(DefaultGeneratorParametersBytes, DefaultGeneratorParameters); err != nil {
		panic(err)
	}
}
