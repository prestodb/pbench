package gen_config

import (
	_ "embed"
	"encoding/json"
)

type GeneratorParameters struct {
	// Container memory = node memory - SysReservedGb
	SysReservedGb                        float64 `json:"sys_reserved_gb"`
	HeapSizePercentOfContainerMem        float64 `json:"heap_size_percent_of_container_mem"`
	HeadroomPercentOfHeap                float64 `json:"headroom_percent_of_heap"`
	QueryMaxTotalMemPerNodePercentOfHeap float64 `json:"query_max_total_mem_per_node_percent_of_heap"`
	QueryMaxMemPerNodePercentOfTotal     float64 `json:"query_max_mem_per_node_percent_of_total"`
	ProxygenMemPerWorkerGb               float64 `json:"proxygen_mem_per_worker_gb"`
	ProxygenMemCapGb                     float64 `json:"proxygen_mem_cap_gb"`
	NonVeloxBufferMemGb                  float64 `json:"non_velox_buffer_mem_gb"`
	NativeQueryMemPercentOfSysMem        float64 `json:"native_query_mem_percent_of_sys_mem"`
}

var (
	//go:embed default_params.json
	DefaultGeneratorParametersBytes []byte
	DefaultGeneratorParameters      = &GeneratorParameters{}
)

func init() {
	if err := json.Unmarshal(DefaultGeneratorParametersBytes, DefaultGeneratorParameters); err != nil {
		panic(err)
	}
}
