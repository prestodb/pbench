package gen_config

import (
	"math"
)

type ClusterConfig struct {
	Name                          string               `json:"cluster_size"`
	WorkerInstanceType            string               `json:"worker_instance_type"`
	NumberOfWorkers               uint                 `json:"number_of_workers"`
	MemoryPerNodeGb               uint                 `json:"memory_per_node_gb"`
	VCPUPerWorker                 uint                 `json:"vcpu_per_worker"`
	GeneratorParameters           *GeneratorParameters `json:"generator_parameters,omitempty"`
	SystemReservedGb              uint                 `json:"-"`
	ContainerMemoryGb             uint                 `json:"-"`
	HeadroomGb                    uint                 `json:"-"`
	HeapSizeGb                    uint                 `json:"-"`
	JavaQueryMaxTotalMemPerNodeGb uint                 `json:"-"`
	JavaQueryMaxMemPerNodeGb      uint                 `json:"-"`
	NativeSystemMemGb             uint                 `json:"-"`
	NativeQueryMemGb              uint                 `json:"-"`
	Path                          string               `json:"-"`
}

func (c *ClusterConfig) Calculate() {
	c.SystemReservedGb = uint(
		math.Min(
			math.Max(math.Round(float64(c.MemoryPerNodeGb)*c.GeneratorParameters.SysReservedPercent),
				c.GeneratorParameters.MinSysReservedGb),
			c.GeneratorParameters.MaxSysReservedGb))
	c.ContainerMemoryGb = c.MemoryPerNodeGb - c.SystemReservedGb
	c.HeapSizeGb = uint(math.Round(float64(c.ContainerMemoryGb) * c.GeneratorParameters.HeapSizePercentOfContainerMem))
	c.HeadroomGb = uint(math.Round(float64(c.HeapSizeGb) * c.GeneratorParameters.HeadroomPercentOfHeap))
	c.JavaQueryMaxTotalMemPerNodeGb = uint(math.Round(float64(c.HeapSizeGb) * c.GeneratorParameters.QueryMaxTotalMemPerNodePercentOfHeap))
	c.JavaQueryMaxMemPerNodeGb = uint(math.Round(float64(c.JavaQueryMaxTotalMemPerNodeGb) * c.GeneratorParameters.QueryMaxMemPerNodePercentOfTotal))
	c.NativeSystemMemGb = uint(math.Round(float64(c.ContainerMemoryGb) * c.GeneratorParameters.NativeSysMemPercentOfContainerMem))
	c.NativeQueryMemGb = uint(math.Round(float64(c.NativeSystemMemGb) * c.GeneratorParameters.NativeQueryMemPercentOfSysMem))
}
