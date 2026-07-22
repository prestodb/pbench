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

package diagnosis

import (
	"fmt"
	"os"
	"testing"

	"github.com/prestodb/presto-go-client/v2/queryjson"
)

// realSamplePaths are the real-sample locations used for manual validation
// (design.md §8, "Real-sample validation assets"). BenchmarkDiagnose skips
// gracefully when neither is present (e.g. in an environment without the
// manual validation assets), so `go test -bench` never fails CI on their
// absence.
var realSamplePaths = []string{
	"/Users/yabinma/Downloads/20260718_003937_00077_muisn.json",
	"/tmp/pbench-phase1-test/out/feature/stage_139_346_260718-150403/stage_139_346_139_346.json",
	"/tmp/pbench-cycle/out/stage_139_346_260719-095736/stage_139_346_139_346.json",
}

// BenchmarkDiagnose (B-2): full analysis of the real 9.6MB sample (double
// unmarshal + metrics + classification + markdown render). Target: < 2s/file
// so batch mode stays interactive.
func BenchmarkDiagnose(b *testing.B) {
	var raw []byte
	for _, p := range realSamplePaths {
		if data, err := os.ReadFile(p); err == nil {
			raw = data
			break
		}
	}
	if raw == nil {
		b.Skip("no real sample file available (see realSamplePaths); skipping B-2")
	}
	th := DefaultThresholds()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		report, err := Analyze(raw, th)
		if err != nil {
			b.Fatal(err)
		}
		_ = report.RenderMarkdown(5)
	}
}

// buildSyntheticLargeQuery builds a synthetic query with numOperators
// operators spread across numNodes plan nodes (parse excluded, per B-3's
// spec: this measures the metrics+classification loop, not JSON parsing).
func buildSyntheticLargeQuery(numOperators, numNodes int) (*ExtraInfo, []*queryjson.OperatorSummary) {
	ops := make([]*queryjson.OperatorSummary, 0, numOperators)
	stats := make(map[string]*NodeEstimate, numNodes)
	for i := 0; i < numOperators; i++ {
		nodeId := fmt.Sprintf("%d", i%numNodes)
		stageId := i % 20
		ops = append(ops, &queryjson.OperatorSummary{
			StageId: stageId, PlanNodeId: nodeId, OperatorType: "FilterProject",
			InputPositions: 1000 + i, OutputPositions: 1000 + i,
		})
	}
	for n := 0; n < numNodes; n++ {
		stats[fmt.Sprintf("%d", n)] = &NodeEstimate{Confident: "HIGH", OutputRowCount: 1000}
	}
	return &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: stats}}, ops
}

// BenchmarkClassify (B-3): metrics + classification over a synthetic
// 2,000-operator / 500-node query (parse excluded).
func BenchmarkClassify(b *testing.B) {
	extra, ops := buildSyntheticLargeQuery(2000, 500)
	th := DefaultThresholds()
	qi := &queryjson.QueryInfo{QueryStats: &queryjson.QueryStats{
		OperatorSummaries: ops, TotalCpuTime: queryjson.Duration{}, ElapsedTime: queryjson.Duration{},
	}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeMetrics := ComputeNodeMetrics(extra, ops)
		sig := ExtractSignals(qi, extra, nodeMetrics, nil, nil, nil)
		categories := Classify(sig, th)
		Attribute(categories, sig, th)
	}
}

// BenchmarkProgression (B-4): a 40-frame snapshot series, delta/rate
// computation (using the same in-memory frame construction as the unit
// tests, since the real dry-run dataset only has 20 frames).
func BenchmarkProgression(b *testing.B) {
	th := DefaultThresholds()
	frames := make([]*ParsedFrame, 0, 40)
	for i := 0; i < 40; i++ {
		frames = append(frames, makeFrame(i+1, int64(i)*30000, 100+float64(i)*20, 1000, 50, 1e9, 0, map[int]int64{9: int64(100 + i*20)}))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AnalyzeProgression(frames, nil, th)
	}
}
