package gen_config

import (
	"math"
)

type ClusterConfig struct {
	Name                          string                `json:"name"`
	WorkerInstanceType            string                `json:"worker_instance_type"`
	NumberOfWorkers               uint                  `json:"number_of_workers"`
	MemoryPerNodeGb               uint                  `json:"memory_per_node_gb"`
	VCPUPerWorker                 uint                  `json:"vcpu_per_worker"`
	GenerationParameters          *GenerationParameters `json:"generation_parameters,omitempty"`
	SystemReservedGb              uint                  `json:"-"`
	ContainerMemoryGb             uint                  `json:"-"`
	HeadroomGb                    uint                  `json:"-"`
	HeapSizeGb                    uint                  `json:"-"`
	JavaQueryMaxTotalMemPerNodeGb uint                  `json:"-"`
	JavaQueryMaxMemPerNodeGb      uint                  `json:"-"`
	NativeSystemMemGb             uint                  `json:"-"`
	NativeQueryMemGb              uint                  `json:"-"`
	Path                          string                `json:"-"`
}

func (c *ClusterConfig) Calculate() {
	if c.GenerationParameters == nil {
		c.GenerationParameters = DefaultGenerationParameters
	}
	c.SystemReservedGb = uint(math.Max(math.Round(float64(c.MemoryPerNodeGb)*c.GenerationParameters.SysReservedPercent), c.GenerationParameters.MinSysReservedGb))
	c.ContainerMemoryGb = c.MemoryPerNodeGb - c.SystemReservedGb
	c.HeapSizeGb = uint(math.Round(float64(c.ContainerMemoryGb) * c.GenerationParameters.HeapSizePercentOfContainerMem))
	c.HeadroomGb = uint(math.Round(float64(c.HeapSizeGb) * c.GenerationParameters.HeadroomPercentOfHeap))
	c.JavaQueryMaxTotalMemPerNodeGb = uint(math.Round(float64(c.HeapSizeGb) * c.GenerationParameters.QueryMaxTotalMemPerNodePercentOfHeap))
	c.JavaQueryMaxMemPerNodeGb = uint(math.Round(float64(c.JavaQueryMaxTotalMemPerNodeGb) * c.GenerationParameters.QueryMaxMemPerNodePercentOfTotal))
	c.NativeSystemMemGb = uint(math.Round(float64(c.ContainerMemoryGb) * c.GenerationParameters.NativeSysMemPercentOfContainerMem))
	c.NativeQueryMemGb = uint(math.Round(float64(c.NativeSystemMemGb) * c.GenerationParameters.NativeQueryMemPercentOfSysMem))
}
