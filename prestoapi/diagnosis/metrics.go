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
	"math"
	"sort"

	"github.com/prestodb/presto-go-client/v2/queryjson"
)

// NodeMetrics holds the per-plan-node derived metrics (FP-10, formulas per
// design §6.3 with one deviation documented below).
//
// JSON tags (round-4 fix, design.md §10 item 13/§5.5's locked canonical
// schema): every field the report actually surfaces gets a camelCase tag, so
// the JSON output is uniformly camelCase like the rest of the report -- this
// struct previously had NO tags at all, leaking Go's PascalCase field names
// verbatim (ActualFanout/PlanNodeId/Confidence/MinVariableNDV/...). Field
// values/semantics/severity are entirely unchanged; this is a serialization
// fix only.
type NodeMetrics struct {
	PlanNodeId         string   `json:"planNodeId"`
	OperatorTypes      []string `json:"operatorTypes"`
	ActualIn           *int64   `json:"actualIn"`           // nil when unavailable (no terminal/finished-stage actual, §2)
	ActualOut          *int64   `json:"actualOut"`          // nil when unavailable
	ActualFanout       *float64 `json:"fanout"`             // nil when unavailable
	EstimatedOut       *float64 `json:"estimatedOut"`       // nil when NaN/absent
	Confidence         string   `json:"confidence"`         // LOW|HIGH|FACT|"" (planStatsAndCosts.confident)
	MinVariableNDV     *float64 `json:"minNdv"`             // nil when all NaN/absent across variableStatistics
	EstimateErrorRatio *float64 `json:"estimateErrorRatio"` // actualOut/estimatedOut; nil when either side is unavailable
	IsLowConfidence    bool     `json:"isLowConfidence"`

	// HasJoinStats/AllNaNJoinStats are joinNodeStatsEstimate-derived internal
	// flags (REC-2: that field is a non-causal HBO/null-skew signal, source-
	// verified NOT a CBO cardinality input) -- hidden from JSON (round-4 fix)
	// so they never surface as causal-looking booleans in the report; the
	// md informational line (report.go) still reads them internally as plain
	// Go fields, unaffected by the json tag.
	HasJoinStats    bool `json:"-"`
	AllNaNJoinStats bool `json:"-"`

	// PipelineId/OperatorId are the max-output operator's own physical
	// position (from the SAME operatorSummary entry ActualIn/ActualOut/
	// OperatorTypes were derived from), nil when unavailable. Used only by
	// ExtractFanoutChain (REC-3) to break ties when two candidate nodes in a
	// chain happen to report the exact same actual value (e.g. a 1:1
	// passthrough hop's output coincidentally equals its own predecessor's
	// output) -- operatorId is monotonic in physical execution order within
	// one pipeline, so it disambiguates what raw in/out value-matching alone
	// cannot. Not part of any severity/causal-engine decision; hidden from
	// JSON (round-4 fix) as a purely internal tie-break helper.
	PipelineId *int `json:"-"`
	OperatorId *int `json:"-"`

	// JoinKeyStats and EstimateProvenance are the cycle-test evidence
	// enrichments (design.md §10 item 10, §5.4.1 A3): both are populated only
	// on the A3 verdict's winning node (never universally across all nodes),
	// by analyze.go's enrichA3Evidence after the causal engine has already
	// picked the root — additive "what+where" evidence, no severity effect.
	JoinKeyStats []JoinKeyStat `json:"joinKeyStats,omitempty"`
	// EstimateProvenance is "default_join_selectivity_fallback" when the
	// estimate is LOW-confidence, has >=1 NaN-NDV join key, and
	// outputRowCount >= FallbackEstimateMin (display/labeling only); absent
	// (omitted from JSON) otherwise.
	EstimateProvenance string `json:"estimateProvenance,omitempty"`
}

// ComputeNodeMetrics builds one NodeMetrics per plan-node id, unioning the ids
// present in planStatsAndCosts.stats and in operatorSummaries[].planNodeId
// (§6.3 grain), from a single frame's operator summaries and plan estimates.
//
// Actual-side aggregation deviates from the literal "sum(InputPositions) across
// probe+build sides" prose in design.md §6.3: verified against the real sample,
// sum(InputPositions) over ALL operators sharing a node id over-counts even in
// the non-join pass-through case (a FilterProject and its downstream
// PartitionedOutput operator commonly share one plan-node id with identical
// in==out; summing their inputs would report fanout 0.5 instead of 1.0), and for
// the motivating join node it yields fan-out ~631x rather than the ~744x the
// design's own acceptance criterion (FP-15) requires. The actual (also
// self-consistent) formula is: actual_output = max(OutputPositions) across
// operators sharing the node id (as already specified, to avoid double-counting
// build-side operators that output 0 rows); actual_input = the SUM of
// InputPositions of only the operator(s) that achieve that max output (i.e. the
// "primary"/probe side that produced the node's real output), which collapses to
// the single dominant operator's own input in both verified real cases.
func ComputeNodeMetrics(extra *ExtraInfo, operatorSummaries []*queryjson.OperatorSummary) map[string]*NodeMetrics {
	result := map[string]*NodeMetrics{}

	type agg struct {
		maxOut        int64
		inAtMax       int64
		pipelineAtMax int
		operatorAtMax int
		isJoinAtMax   bool
		types         map[string]bool
		hasSummary    bool
		hasMax        bool
	}
	aggByNode := map[string]*agg{}
	for _, op := range operatorSummaries {
		if op == nil || op.PlanNodeId == "" {
			continue
		}
		a := aggByNode[op.PlanNodeId]
		if a == nil {
			a = &agg{types: map[string]bool{}}
			aggByNode[op.PlanNodeId] = a
		}
		a.hasSummary = true
		a.types[op.OperatorType] = true
		out := int64(op.OutputPositions)
		in := int64(op.InputPositions)
		isJoin := isJoinOperatorType(op.OperatorType)
		switch {
		case !a.hasMax || out > a.maxOut:
			a.maxOut = out
			a.inAtMax = in
			a.pipelineAtMax = op.PipelineId
			a.operatorAtMax = op.OperatorId
			a.isJoinAtMax = isJoin
			a.hasMax = true
		case out == a.maxOut && isJoin && !a.isJoinAtMax:
			// Round-8 fix (§5.4.1 A3(iii)(ι) R8-C annihilation rows): when a
			// join's probe (LookupJoin) and build (HashBuilder) operators tie
			// at output 0 -- the exact shape of an annihilated join, where
			// the build side measured empty and the probe's own huge input
			// never got through -- prefer the JOIN (probe) operator's own
			// in/out unconditionally over the build operator's, regardless of
			// the min-input rule below: the join's TRUE measured in/out
			// (e.g. 445,201,868,645 -> 0) is what the fan-out chain and its
			// annihilation evidence line need, never the build side's
			// unrelated 0 -> 0.
			a.inAtMax = in
			a.pipelineAtMax = op.PipelineId
			a.operatorAtMax = op.OperatorId
			a.isJoinAtMax = true
		case out == a.maxOut && isJoin == a.isJoinAtMax && in < a.inAtMax:
			// Tie on output AND on "joinness" (e.g. a FilterProject + its
			// downstream PartitionedOutput commonly share one node id with
			// identical in==out, verified against real data): take the
			// smaller input among the tied operators rather than summing,
			// which would double-count and report an artificially low
			// fan-out (e.g. 0.5 instead of 1.0 for a pure pass-through node).
			// The choice of min (vs. first-seen) keeps the result independent
			// of operator iteration order.
			a.inAtMax = in
			a.pipelineAtMax = op.PipelineId
			a.operatorAtMax = op.OperatorId
		}
	}

	get := func(id string) *NodeMetrics {
		if m, ok := result[id]; ok {
			return m
		}
		m := &NodeMetrics{PlanNodeId: id}
		result[id] = m
		return m
	}

	for id, a := range aggByNode {
		m := get(id)
		types := make([]string, 0, len(a.types))
		for t := range a.types {
			types = append(types, t)
		}
		sort.Strings(types)
		m.OperatorTypes = types
		if a.hasSummary {
			out := a.maxOut
			in := a.inAtMax
			pipelineId := a.pipelineAtMax
			operatorId := a.operatorAtMax
			m.ActualOut = &out
			m.ActualIn = &in
			m.PipelineId = &pipelineId
			m.OperatorId = &operatorId
			if in > 0 {
				fanout := float64(out) / float64(in)
				m.ActualFanout = &fanout
			} else if out > 0 {
				// Input unmeasured/zero but output positive: fan-out is
				// unbounded/unavailable rather than a divide-by-zero artifact.
				m.ActualFanout = nil
			}
		}
	}

	if extra != nil && extra.PlanStatsAndCosts != nil {
		for id, est := range extra.PlanStatsAndCosts.Stats {
			if est == nil {
				continue
			}
			m := get(id)
			m.Confidence = est.Confident
			m.IsLowConfidence = est.Confident == "LOW"
			if !isNaN(est.OutputRowCount) {
				v := float64(est.OutputRowCount)
				m.EstimatedOut = &v
			}
			if minNdv, found := MinDistinctValuesCount(est.VariableStatistics); found {
				m.MinVariableNDV = &minNdv
			}
			if est.JoinNodeStatsEstimate != nil {
				m.HasJoinStats = true
				m.AllNaNJoinStats = est.JoinNodeStatsEstimate.AllNaN()
			}
		}
	}

	for _, m := range result {
		if m.ActualOut != nil && m.EstimatedOut != nil && *m.EstimatedOut != 0 && !math.IsNaN(*m.EstimatedOut) {
			ratio := float64(*m.ActualOut) / *m.EstimatedOut
			m.EstimateErrorRatio = &ratio
		}
	}
	return result
}
