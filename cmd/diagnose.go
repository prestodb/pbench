// Copyright 2026.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"pbench/cmd/diagnose"
)

// diagnoseCmd represents the `pbench diagnose` command (design.md §5.2).
var diagnoseCmd = &cobra.Command{
	Use:   `diagnose [flags] <base>.json | <base>.snapshots/ | <run-output-dir>/ ...`,
	Short: "Diagnose a query's bottleneck from its query JSON (and Phase-1 snapshots)",
	Long: `Read query JSON (a terminal file, a Phase-1 .snapshots/ directory, or a run
output directory to batch) and print a deterministic bottleneck-diagnosis
report: the taxonomy category, the specific plan node/operator/stage, and
ranked numeric evidence for the root cause and any attributed symptoms.

pbench diagnose is deterministic code, not an LLM: it delivers what + where.
Recommendations, when printed, are obvious templated one-liners from a fixed
lookup table -- never synthesized analysis. The tool never modifies the input
query JSON; reports go to stdout and, optionally, a separate sidecar file.`,
	DisableFlagsInUseLine: true,
	Args: func(_ *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("requires at least 1 arg (a query JSON file, a .snapshots directory, or a run output directory), only received %d", len(args))
		}
		return nil
	},
	Run: diagnose.Run,
}

func init() {
	RootCmd.AddCommand(diagnoseCmd)
	diagnoseCmd.Flags().StringVarP(&diagnose.Format, "format", "f", "md", "Report format: md | json")
	diagnoseCmd.Flags().BoolVarP(&diagnose.Sidecar, "sidecar", "s", false, "Also write <input>.analysis.<md|json> next to the input")
	diagnoseCmd.Flags().StringVarP(&diagnose.OutputPath, "output-path", "o", "", "Write reports into this directory instead of sidecar placement")
	diagnoseCmd.Flags().BoolVar(&diagnose.NoSnapshots, "no-snapshots", false, "Ignore sibling .snapshots/ directories")
	diagnoseCmd.Flags().StringVar(&diagnose.QueryIdFilter, "query-id", "", "Batch mode: only diagnose files whose JSON queryId matches")
	diagnoseCmd.Flags().IntVar(&diagnose.Top, "top", 5, "Evidence rows per category")
	diagnoseCmd.Flags().StringVar(&diagnose.ThresholdsPath, "thresholds", "", "JSON file overriding classification thresholds")
}
