package genconfig

import (
	"math"
)

type ClusterConfig struct {
	Name                          string               `json:"cluster_size"`
	WorkerInstanceType            string               `json:"worker_instance_type"`
	NumberOfWorkers               uint                 `json:"number_of_workers"`
	MemoryPerNodeGb               uint                 `json:"memory_per_node_gb"`
	VCPUPerWorker                 uint                 `json:"vcpu_per_worker"`
	SpillEnabled                  bool                 `json:"spill_enabled,omitempty"`
	SsdCacheSize                  uint                 `json:"ssd_cache_size,omitempty"`
	GeneratorParameters           *GeneratorParameters `json:"generator_parameters,omitempty"`
	ContainerMemoryGb             uint                 `json:"-"`
	HeadroomGb                    uint                 `json:"-"`
	HeapSizeGb                    uint                 `json:"-"`
	JavaQueryMaxTotalMemPerNodeGb uint                 `json:"-"`
	JavaQueryMaxMemPerNodeGb      uint                 `json:"-"`
	NativeSystemMemGb             uint                 `json:"-"`
	NativeProxygenMemGb           uint                 `json:"-"`
	NativeBufferMemGb             uint                 `json:"-"`
	NativeQueryMemGb              uint                 `json:"-"`
	JoinMaxBroadcastTableSizeMb   uint                 `json:"-"`
	Path                          string               `json:"-"`
	FragmentResultCacheSizeGb     uint                 `json:"-"`
	FragmentCacheEnabled          bool                 `json:"fragment_result_cache_enabled"`
	DataCacheSizeGb               uint                 `json:"-"`
	DataCacheEnabled              bool                 `json:"data_cache_enabled"`
}

func (c *ClusterConfig) Calculate() {
	c.ContainerMemoryGb = c.MemoryPerNodeGb - uint(math.Ceil(math.Min(c.GeneratorParameters.SysReservedMemCapGb,
		float64(c.MemoryPerNodeGb)*c.GeneratorParameters.SysReservedMemPercent)))
	c.HeapSizeGb = uint(math.Floor(float64(c.ContainerMemoryGb) * c.GeneratorParameters.HeapSizePercentOfContainerMem))
	c.HeadroomGb = uint(math.Ceil(float64(c.HeapSizeGb) * c.GeneratorParameters.HeadroomPercentOfHeap))
	c.JavaQueryMaxTotalMemPerNodeGb = uint(math.Floor(float64(c.HeapSizeGb) * c.GeneratorParameters.QueryMaxTotalMemPerNodePercentOfHeap))
	c.JavaQueryMaxMemPerNodeGb = uint(math.Floor(float64(c.JavaQueryMaxTotalMemPerNodeGb) * c.GeneratorParameters.QueryMaxMemPerNodePercentOfTotal))
	c.NativeProxygenMemGb = uint(math.Ceil(math.Min(c.GeneratorParameters.ProxygenMemCapGb, c.GeneratorParameters.ProxygenMemPerWorkerGb*float64(c.NumberOfWorkers))))
	c.NativeBufferMemGb = uint(math.Ceil(math.Min(c.GeneratorParameters.NativeBufferMemCapGb, float64(c.ContainerMemoryGb)*c.GeneratorParameters.NativeBufferMemPercent)))
	c.NativeSystemMemGb = c.ContainerMemoryGb - c.NativeBufferMemGb - c.NativeProxygenMemGb
	c.NativeQueryMemGb = uint(math.Floor(float64(c.NativeSystemMemGb) * c.GeneratorParameters.NativeQueryMemPercentOfSysMem))
	c.JoinMaxBroadcastTableSizeMb = uint(math.Ceil(float64(c.ContainerMemoryGb) * c.GeneratorParameters.JoinMaxBcastSizePercentOfContainerMem * 1024))
	c.FragmentResultCacheSizeGb = uint(math.Ceil(float64(c.MemoryPerNodeGb*2) * float64(0.95)))
	c.DataCacheSizeGb = uint(math.Ceil(float64(c.MemoryPerNodeGb*3) * float64(0.95)))
}
