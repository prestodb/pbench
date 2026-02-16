// Copyright 2024.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queryplan

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"pbench/log"
	"pbench/prestoapi/plan_node"

	"github.com/spf13/cobra"
)

var (
	queryPlanColumn int
	hasHeader       bool
	output          string // Note: shadowed by *os.File in processFile() line 85 — use caution when renaming.
	failureCounter  = 0
	validCounter    = 0
)

var Cmd *cobra.Command

func init() {
	Cmd = &cobra.Command{
		Use:                   `queryplan [flags] <CSV file>`,
		Short:                 "Parse query plan",
		Long:                  `Read a CSV file, parse the "query plan" column, and write the JOIN information into a JSON file`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(1),
		Run:                   run,
	}
	Cmd.Flags().IntVarP(&queryPlanColumn, "column", "c", 0, `The column index for the Query Plans in the CSV file(index starts with 0)`)
	Cmd.Flags().StringVarP(&output, "output", "o", "queryplan.json", "Output JSON file")
	Cmd.Flags().BoolVarP(&hasHeader, "has-header", "s", true, "contain the header line or not")
}

func run(c *cobra.Command, args []string) {
	c.ValidateRequiredFlags()
	csvFile := args[0]

	log.Info().Msgf("parsing the query plan at column %d in %s", queryPlanColumn, csvFile)
	if err := processFile(csvFile); err != nil {
		log.Fatal().Err(err).Msg("failed to process the CSV file")
	}
	log.Info().Msgf("Join information is stored to %s in JSON format, using row number as the key", output)
	if failureCounter > 0 {
		fmt.Printf("failed to parse %d records out of %d records\n", failureCounter, validCounter)
	}
}

func processFile(csvFile string) error {
	f, err := os.Open(csvFile)
	if err != nil {
		return err
	}

	defer f.Close()

	var r = csv.NewReader(f)
	var rowNum = 1

	if hasHeader {
		if _, err := r.Read(); err != nil {
			log.Fatal().Err(err).Msg("failed to consume the header line")
		}
		rowNum++
	}

	output, err := os.Create(output)
	if err != nil {
		return err
	}
	defer output.Close()

	if _, err := output.WriteString("{\n"); err != nil {
		return err
	}

	var newline = ""
	for ; ; rowNum++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if record[queryPlanColumn] == "" {
			log.Info().Msgf("empty query plan at row:%d", rowNum)
			continue
		}

		validCounter++
		if joins, err := parseQueryPlan(record[queryPlanColumn]); err != nil {
			log.Err(err).Msgf("failed to parse the query plan at row:%d", rowNum)
			failureCounter++
		} else if len(joins) > 0 {
			if out, err := json.MarshalIndent(joins, "  ", "  "); err != nil {
				log.Err(err).Msgf("failed to serialize the joins at row:%d", rowNum)
				failureCounter++
			} else {
				output.WriteString(fmt.Sprintf(`%s  "%d":`, newline, rowNum))
				fmt.Fprint(output, string(out))
				newline = ",\n"
			}
		}
	}
	output.WriteString("\n}")
	return nil
}

func parseQueryPlan(planStr string) ([]plan_node.Join, error) {
	planTree := make(plan_node.PlanTree)
	if err := json.Unmarshal([]byte(planStr), &planTree); err != nil {
		return nil, err
	}

	return planTree.ParseJoins()
}
