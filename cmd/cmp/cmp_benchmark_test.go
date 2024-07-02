package cmp

import (
	"github.com/spf13/cobra"
	"testing"
)

var cmpCmd = &cobra.Command{
	Use:                   `cmp [flags] [directory 1] [directory 2]`,
	DisableFlagsInUseLine: true,
	Run:                   Run,
	Args:                  cobra.ExactArgs(2),
	Short:                 "Compare two query result directories",
}

func BenchmarkRun(b *testing.B) {
	// Set up your test data
	folder1 := "/Users/allen/Documents/test1"
	folder2 := "/Users/allen/Documents/test2"

	// Create a dummy cobra.Command

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Run(cmpCmd, []string{folder1, folder2})
	}
}
