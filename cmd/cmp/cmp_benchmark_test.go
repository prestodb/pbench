package cmp

import (
	"github.com/spf13/cobra"
	"testing"
)

func BenchmarkRun(b *testing.B) {
	// Set up your test data
	folder1 := "/Users/allen/Documents/test1/"
	folder2 := "/Users/allen/Documents/test1/"

	// Create a dummy cobra.Command
	cmd := &cobra.Command{}

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Run(cmd, []string{folder1, folder2})
	}
}
