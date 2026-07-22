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

// Package diagnose implements the `pbench diagnose` CLI (design.md §5.2):
// flags, the three input modes (terminal file / .snapshots dir / batch
// run-output dir), and report output (stdout + optional sidecar). All the
// analysis logic lives in prestoapi/diagnosis; this package is orchestration
// only.
package diagnose

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"pbench/log"
	"pbench/prestoapi/diagnosis"
	"pbench/utils"
)

var (
	Format         string
	Sidecar        bool
	OutputPath     string
	NoSnapshots    bool
	QueryIdFilter  string
	Top            int
	ThresholdsPath string
)

// Run is the cobra Run function for `pbench diagnose` (registered by
// cmd/diagnose.go, mirroring cmd/loadjson.go).
func Run(_ *cobra.Command, args []string) {
	th, err := diagnosis.LoadThresholds(ThresholdsPath)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load --thresholds override file")
	}
	// --top governs both the markdown evidence-row limit (rendering) and the
	// NO-DOMINANT-BOTTLENECK top-per-category-readings count (classify.go's
	// topCategoryReadings), so wire the CLI flag into Thresholds.Top rather
	// than leaving that field disconnected from the flag that documents it.
	th.Top = Top

	analyzed := 0
	failed := 0
	for _, arg := range args {
		utils.ExpandHomeDirectory(&arg)
		n, err := processInput(arg, th)
		analyzed += n
		if err != nil {
			failed++
			log.Error().Str("path", arg).Err(err).Msg("failed to process input")
		}
	}
	// Exit codes (design.md §10 OQ-5): 0 = analyzed OK (regardless of verdict),
	// 1 = no analyzable input at all.
	if analyzed == 0 {
		log.Error().Msg("no analyzable input")
		os.Exit(1)
	}
	if failed > 0 {
		log.Warn().Int("failed_inputs", failed).Msg("some inputs could not be processed; see above")
	}
}

// processInput dispatches to the correct input mode (§5.2) and returns the
// number of queries successfully analyzed.
func processInput(path string, th *diagnosis.Thresholds) (int, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if !stat.IsDir() {
		return processSingleFile(path, th)
	}
	if isSnapshotsDirPath(path) {
		return processSingleSnapshotsDir(path, th)
	}
	return processBatchDir(path, th)
}

// isSnapshotsDirPath reports whether path is itself a `<base>.snapshots`
// directory (Mode 2), as opposed to a run-output directory to batch (Mode 3).
func isSnapshotsDirPath(path string) bool {
	clean := strings.TrimRight(path, string(filepath.Separator))
	return strings.HasSuffix(clean, ".snapshots")
}

// siblingSnapshotsDir implements the exact, mechanical Mode-1 auto-discovery
// rule (§5.2): strip the ".json" extension from the terminal file's path and
// append ".snapshots".
func siblingSnapshotsDir(jsonPath string) string {
	if strings.HasSuffix(jsonPath, ".json") {
		return strings.TrimSuffix(jsonPath, ".json") + ".snapshots"
	}
	return jsonPath + ".snapshots"
}

// discoverSiblingFrames returns the sibling snapshot frames for a terminal
// file, or nil when --no-snapshots is set or no sibling directory exists
// (first-class no-snapshot mode, FP-13).
func discoverSiblingFrames(jsonPath string) []*diagnosis.FrameFile {
	if NoSnapshots {
		return nil
	}
	dir := siblingSnapshotsDir(jsonPath)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil
	}
	files, warnings := diagnosis.DiscoverFrames(dir)
	for _, w := range warnings {
		log.Warn().Str("snapshots_dir", dir).Msg(w)
	}
	return files
}

// analyzeFile reads and analyzes one terminal query JSON file, with sibling
// snapshot auto-discovery (Mode 1).
func analyzeFile(path string, th *diagnosis.Thresholds) (*diagnosis.Report, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if frames := discoverSiblingFrames(path); len(frames) > 0 {
		return diagnosis.AnalyzeWithSnapshots(raw, frames, th)
	}
	return diagnosis.Analyze(raw, th)
}

func processSingleFile(path string, th *diagnosis.Thresholds) (int, error) {
	report, err := analyzeFile(path, th)
	if err != nil {
		return 0, err
	}
	printReport(report)
	if Sidecar || OutputPath != "" {
		if err := writeSidecar(report, path); err != nil {
			log.Error().Str("path", path).Err(err).Msg("failed to write sidecar report")
		}
	}
	return 1, nil
}

func processSingleSnapshotsDir(dir string, th *diagnosis.Thresholds) (int, error) {
	files, warnings := diagnosis.DiscoverFrames(dir)
	for _, w := range warnings {
		log.Warn().Str("snapshots_dir", dir).Msg(w)
	}
	report, err := diagnosis.AnalyzeSnapshotsOnly(files, th)
	if err != nil {
		return 0, err
	}
	printReport(report)
	if Sidecar || OutputPath != "" {
		if err := writeSidecar(report, dir); err != nil {
			log.Error().Str("path", dir).Err(err).Msg("failed to write sidecar report")
		}
	}
	return 1, nil
}

// isSidecarOrAuxFile excludes pbench's own generated sidecars/aux files from
// batch discovery (§5.2), mirroring loadjson's exclusions.
func isSidecarOrAuxFile(name string) bool {
	for _, suffix := range []string{".error.json", ".cols.json", ".analysis.json", ".analysis.md"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

type batchRow struct {
	path       string
	category   string
	nodeId     string
	confidence string
	severity   int
}

// processBatchDir implements Mode 3 (FP-14): a non-recursive walk (mirroring
// loadjson's processPath — .snapshots/ subdirectories are never descended into
// unless given directly as Mode 2), auto-pairing each terminal file with its
// sibling snapshots dir, printing one verdict line per query plus a summary
// table.
func processBatchDir(dir string, th *diagnosis.Thresholds) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var rows []batchRow
	count := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || isSidecarOrAuxFile(e.Name()) {
			continue
		}
		full := filepath.Join(dir, e.Name())
		raw, readErr := os.ReadFile(full)
		if readErr != nil {
			log.Warn().Str("path", full).Err(readErr).Msg("skipping unreadable file")
			continue
		}
		q, parseErr := diagnosis.ParseQuery(raw)
		if parseErr != nil {
			log.Warn().Str("path", full).Err(parseErr).Msg("skipping invalid query JSON")
			continue
		}
		if QueryIdFilter != "" && q.Info.QueryId != QueryIdFilter {
			continue
		}
		var report *diagnosis.Report
		if frames := discoverSiblingFrames(full); len(frames) > 0 {
			report, err = diagnosis.AnalyzeWithSnapshots(raw, frames, th)
		} else {
			report, err = diagnosis.Analyze(raw, th)
		}
		if err != nil {
			log.Warn().Str("path", full).Err(err).Msg("failed to analyze")
			continue
		}
		count++
		printVerdictLine(full, report)
		row := batchRow{path: full, confidence: report.Verdict.Confidence.Level}
		if report.Verdict.Category == "" {
			row.category = report.Verdict.CategoryName
		} else {
			row.category = fmt.Sprintf("%s %s", report.Verdict.Category, report.Verdict.CategoryName)
			row.nodeId = report.Verdict.NodeId
			for _, c := range report.Categories {
				if c.Category == report.Verdict.Category {
					row.severity = c.Severity
				}
			}
		}
		rows = append(rows, row)
		if Sidecar || OutputPath != "" {
			if err := writeSidecar(report, full); err != nil {
				log.Error().Str("path", full).Err(err).Msg("failed to write sidecar report")
			}
		}
	}
	printSummaryTable(rows)
	return count, nil
}

func printVerdictLine(path string, report *diagnosis.Report) {
	fmt.Printf("%s: %s (confidence %s)\n", path, report.Verdict.CategoryName, report.Verdict.Confidence.Level)
}

func printSummaryTable(rows []batchRow) {
	if len(rows) == 0 {
		fmt.Println("no queries analyzed")
		return
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].path < rows[j].path })
	fmt.Println("\n| Query file | Verdict | Node | Severity | Confidence |")
	fmt.Println("|---|---|---|---|---|")
	for _, r := range rows {
		fmt.Printf("| %s | %s | %s | %d | %s |\n", filepath.Base(r.path), r.category, r.nodeId, r.severity, r.confidence)
	}
}

func printReport(report *diagnosis.Report) {
	fmt.Println(renderReport(report))
}

func renderReport(report *diagnosis.Report) string {
	if Format == "json" {
		b, err := report.RenderJSON()
		if err != nil {
			log.Error().Err(err).Msg("failed to render JSON report")
			return ""
		}
		return string(b)
	}
	return report.RenderMarkdown(Top)
}

// sidecarPathFor implements the §5.5 sidecar naming rule:
// <input-basename>.analysis.md|json, next to the input (or under --output-path).
func sidecarPathFor(inputPath string) string {
	base := filepath.Base(inputPath)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	base = strings.TrimSuffix(base, ".snapshots")
	ext := "md"
	if Format == "json" {
		ext = "json"
	}
	dir := OutputPath
	if dir == "" {
		dir = filepath.Dir(inputPath)
	}
	return filepath.Join(dir, base+".analysis."+ext)
}

// writeSidecar never modifies the raw query JSON (§5.7 guardrail): it only
// ever creates a new, separate file.
func writeSidecar(report *diagnosis.Report, inputPath string) error {
	path := sidecarPathFor(inputPath)
	f, err := os.OpenFile(path, utils.OpenNewFileFlags, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(renderReport(report))
	if err == nil {
		log.Info().Str("path", path).Msg("wrote diagnosis sidecar report")
	}
	return err
}
