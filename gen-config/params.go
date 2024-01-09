package gen_config

import (
	_ "embed"
	"encoding/json"
)

type GeneratorParameters struct {
	// Container memory = node memory - SysReservedGb
	SysReservedGb                        uint    `json:"sys_reserved_gb"`
	HeapSizePercentOfContainerMem        float64 `json:"heap_size_percent_of_container_mem"`
	HeadroomPercentOfHeap                float64 `json:"headroom_percent_of_heap"`
	QueryMaxTotalMemPerNodePercentOfHeap float64 `json:"query_max_total_mem_per_node_percent_of_heap"`
	QueryMaxMemPerNodePercentOfTotal     float64 `json:"query_max_mem_per_node_percent_of_total"`
	NativeSysMemPercentOfContainerMem    float64 `json:"native_sys_mem_percent_of_container_mem"`
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
