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

// Package diagnosis implements the `pbench diagnose` analysis library (design.md
// §5): it reads raw Presto/Prestissimo query JSON (terminal files and/or Phase-1
// snapshot frames), computes derived bottleneck signals, and produces a
// deterministic what+where diagnosis report. It is entirely self-contained in
// pbench: no change to github.com/prestodb/presto-go-client/v2 (FP-9).
package diagnosis

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/prestodb/presto-go-client/v2/queryjson"
	"pbench/prestoapi/plan_node"
)

// ExtraInfo models the parts of the query JSON that queryjson.QueryInfo does not
// model: planStatsAndCosts, optimizerInformation, and the per-task stage stats
// needed for skew/GC analysis (§5.3 model.go). The same file bytes are unmarshaled
// twice: once into queryjson.QueryInfo (for common fields), once into ExtraInfo.
type ExtraInfo struct {
	QueryId              string             `json:"queryId"`
	State                string             `json:"state"`
	PlanStatsAndCosts    *PlanStatsAndCosts `json:"planStatsAndCosts"`
	OptimizerInformation []OptimizerInfo    `json:"optimizerInformation"`
	QueryStats           *ExtraQueryStats   `json:"queryStats"`
	OutputStage          *StageNode         `json:"outputStage"`
}

// OptimizerInfo is one entry of the optimizerInformation array.
type OptimizerInfo struct {
	OptimizerName       string `json:"optimizerName"`
	OptimizerTriggered  bool   `json:"optimizerTriggered"`
	OptimizerApplicable *bool  `json:"optimizerApplicable,omitempty"`
	IsCostBased         bool   `json:"isCostBased"`
}

// PlanStatsAndCosts is planStatsAndCosts.stats, keyed by plan-node id.
type PlanStatsAndCosts struct {
	Stats map[string]*NodeEstimate `json:"stats"`
	Costs map[string]*NodeCost     `json:"costs"`
}

// NodeCost is one entry of planStatsAndCosts.costs.
type NodeCost struct {
	CpuCost                 plan_node.JsonFloat64 `json:"cpuCost"`
	MaxMemory               plan_node.JsonFloat64 `json:"maxMemory"`
	MaxMemoryWhenOutputting plan_node.JsonFloat64 `json:"maxMemoryWhenOutputting"`
	NetworkCost             plan_node.JsonFloat64 `json:"networkCost"`
}

// NodeEstimate is one entry of planStatsAndCosts.stats. Note: `confident` here is
// a string enum (LOW/HIGH/FACT), whereas plan_node.PlanEstimate.Confident is a
// bool from the textual plan — hence this separate type (§5.3). Numeric fields
// reuse plan_node.JsonFloat64, which already tolerates the mixed bare-E-notation
// number / quoted "NaN"/"Infinity"/"-Infinity" string forms verified in real data.
type NodeEstimate struct {
	OutputRowCount        plan_node.JsonFloat64                   `json:"outputRowCount"`
	TotalSize             plan_node.JsonFloat64                   `json:"totalSize"`
	Confident             string                                  `json:"confident"`
	VariableStatistics    map[string]plan_node.VariableStatistics `json:"variableStatistics"`
	JoinNodeStatsEstimate *JoinNodeStatsEstimate                  `json:"joinNodeStatsEstimate"`
}

// JoinNodeStatsEstimate is present only on join nodes. All-NaN is a clean "join
// has no key statistics" signal (§2, §5.4.1 A3).
type JoinNodeStatsEstimate struct {
	NullJoinBuildKeyCount plan_node.JsonFloat64 `json:"nullJoinBuildKeyCount"`
	JoinBuildKeyCount     plan_node.JsonFloat64 `json:"joinBuildKeyCount"`
	NullJoinProbeKeyCount plan_node.JsonFloat64 `json:"nullJoinProbeKeyCount"`
	JoinProbeKeyCount     plan_node.JsonFloat64 `json:"joinProbeKeyCount"`
}

// AllNaN reports whether every field of the join-stats estimate is NaN, i.e. the
// join had no key statistics at all.
func (j *JoinNodeStatsEstimate) AllNaN() bool {
	if j == nil {
		return false
	}
	return isNaN(j.NullJoinBuildKeyCount) && isNaN(j.JoinBuildKeyCount) &&
		isNaN(j.NullJoinProbeKeyCount) && isNaN(j.JoinProbeKeyCount)
}

// MinDistinctValuesCount returns the minimum distinctValuesCount across all
// variable statistics for a node, and whether any non-NaN value was found.
func MinDistinctValuesCount(vs map[string]plan_node.VariableStatistics) (float64, bool) {
	min := math.Inf(1)
	found := false
	for _, v := range vs {
		f := float64(v.DistinctValuesCount)
		if math.IsNaN(f) {
			continue
		}
		found = true
		if f < min {
			min = f
		}
	}
	return min, found
}

func isNaN(f plan_node.JsonFloat64) bool { return math.IsNaN(float64(f)) }

// StripEstimates removes every "estimates" key from a JSON plan-node tree
// (recursively, at any depth) before it is unmarshaled into plan_node.PlanNode
// / plan_node.PlanTree. Verified against real native-engine query JSON: the
// embedded per-node `estimates[].confident` field is a STRING enum
// ("LOW"/"HIGH"/"FACT") there, whereas plan_node.PlanEstimate.Confident is a
// bool (built for a different, textual plan source) — the type mismatch
// otherwise breaks json.Unmarshal partway through the tree. Diagnosis does not
// need the embedded estimates at all (planStatsAndCosts is the authoritative,
// NaN-tolerant source for the same information), so they are dropped
// defensively rather than risking a partially-populated tree. On any
// marshal/unmarshal failure, the original bytes are returned unchanged.
func StripEstimates(raw []byte) []byte {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	stripEstimatesValue(v)
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

func stripEstimatesValue(v any) {
	switch t := v.(type) {
	case map[string]any:
		delete(t, "estimates")
		for _, child := range t {
			stripEstimatesValue(child)
		}
	case []any:
		for _, child := range t {
			stripEstimatesValue(child)
		}
	}
}

// RuntimeMetric is the wire format of one runtimeStats entry (§2, §5.3
// runtime_stats.go): {name, unit ("NANO"|"BYTE"|"NONE"), sum, count, max, min}.
type RuntimeMetric struct {
	Name  string `json:"name"`
	Unit  string `json:"unit"`
	Sum   int64  `json:"sum"`
	Count int64  `json:"count"`
	Max   int64  `json:"max"`
	Min   int64  `json:"min"`
}

// StageGcStat models one entry of queryStats.stageGcStatistics defensively: the
// exact field-name shape has been observed to vary (`fullGcTasks`/`totalFullGcSec`
// in the verified samples; `fullGcCount`/`fullGcTimeInMillis` documented for other
// coordinator versions per design §2/§5.4.1 D1). All fields are pointers so a
// missing/null field degrades to "no data", never coerced to zero.
type StageGcStat struct {
	StageId            *int     `json:"stageId"`
	Tasks              *int     `json:"tasks"`
	FullGcTasks        *int64   `json:"fullGcTasks"`
	TotalFullGcSec     *float64 `json:"totalFullGcSec"`
	MaxFullGcSec       *float64 `json:"maxFullGcSec"`
	AverageFullGcSec   *float64 `json:"averageFullGcSec"`
	FullGcCount        *int64   `json:"fullGcCount"`
	FullGcTimeInMillis *int64   `json:"fullGcTimeInMillis"`
}

// HasGcData reports whether this entry carries any GC measurement at all (as
// opposed to being entirely absent/null).
func (g *StageGcStat) HasGcData() bool {
	if g == nil {
		return false
	}
	return g.FullGcTasks != nil || g.TotalFullGcSec != nil || g.FullGcCount != nil || g.FullGcTimeInMillis != nil
}

// FullGcTimeSeconds returns the total full-GC time for this stage in seconds, in
// whichever field shape is present.
func (g *StageGcStat) FullGcTimeSeconds() (float64, bool) {
	if g == nil {
		return 0, false
	}
	if g.TotalFullGcSec != nil {
		return *g.TotalFullGcSec, true
	}
	if g.FullGcTimeInMillis != nil {
		return float64(*g.FullGcTimeInMillis) / 1000.0, true
	}
	return 0, false
}

// ExtraQueryStats carries the queryStats fields that queryjson.QueryStats does
// not model (§2): runtimeStats, stageGcStatistics (parsed, not raw), and the
// progress/shuffle/scheduling fields needed by the taxonomy + progression engine.
// Pointer-typed fields distinguish "absent/null" from a real zero.
type ExtraQueryStats struct {
	RuntimeStats                map[string]*RuntimeMetric `json:"runtimeStats"`
	StageGcStatistics           []*StageGcStat            `json:"stageGcStatistics"`
	ProgressPercentage          *float64                  `json:"progressPercentage"`
	ProcessedInputPositions     *int64                    `json:"processedInputPositions"`
	ProcessedInputDataSize      *queryjson.SISize         `json:"processedInputDataSize"`
	ShuffledDataSize            *queryjson.SISize         `json:"shuffledDataSize"`
	ShuffledPositions           *int64                    `json:"shuffledPositions"`
	FullyBlocked                *bool                     `json:"fullyBlocked"`
	BlockedReasons              []string                  `json:"blockedReasons"`
	PeakRunningTasks            *int                      `json:"peakRunningTasks"`
	TotalSplits                 *int                      `json:"totalSplits"`
	QueuedSplits                *int                      `json:"queuedSplits"`
	RunningDrivers              *int                      `json:"runningDrivers"`
	BlockedDrivers              *int                      `json:"blockedDrivers"`
	CompletedDrivers            *int                      `json:"completedDrivers"`
	QueuedTime                  *queryjson.Duration       `json:"queuedTime"`
	ResourceWaitingTime         *queryjson.Duration       `json:"resourceWaitingTime"`
	WaitingForPrerequisitesTime *queryjson.Duration       `json:"waitingForPrerequisitesTime"`
	DispatchingTime             *queryjson.Duration       `json:"dispatchingTime"`
	FinishingTime               *queryjson.Duration       `json:"finishingTime"`
	TotalScheduledTime          *queryjson.Duration       `json:"totalScheduledTime"`
	TotalBlockedTime            *queryjson.Duration       `json:"totalBlockedTime"`
}

// StageNode is a minimal recursive model of outputStage (Presto/older-Trino
// shape), just enough to reach per-task stats (for skew/GC, not modeled by
// queryjson.StageInfo) and each stage's plan fragment (for localizing plan-node
// ids to a stage number, needed by the RunawayComputation live path, §5.6).
type StageNode struct {
	StageId                    string              `json:"stageId"`
	Plan                       *StagePlan          `json:"plan"`
	LatestAttemptExecutionInfo *StageExecutionInfo `json:"latestAttemptExecutionInfo"`
	SubStages                  []*StageNode        `json:"subStages"`
}

// StagePlan carries the raw plan-fragment JSON for a stage.
type StagePlan struct {
	JsonRepresentation string `json:"jsonRepresentation"`
	// Root is the RICH structured plan fragment (round-5, §5.4.1 A3(iii)/§10
	// item 14) — verified present alongside JsonRepresentation, carrying
	// `@type`-tagged nodes with `criteria[]`/`sourceLocation`/`left`/`right`
	// that JsonRepresentation's simplified id/name/identifier/children shape
	// does not retain. Only present per-stage (RemoteSourceNode leaves mark
	// cross-stage boundaries, resolved by RawPlanIndex below); absent-tolerant
	// like every other raw-plan read in this package.
	Root *RawPlanNode `json:"root"`
	// PartitioningScheme (round-8, §5.4.1 A3(iii)(η)/§5.4.5 R8-D) is this
	// FRAGMENT's own OUTPUT partitioning -- a sibling of `root` on the `plan`
	// object, verified present alongside it. R8-D's skewCause resolution
	// reads this from the CHILD (producing) fragment feeding a skewed stage's
	// RemoteSourceNode, per the design's explicit W4 correction: a fragment's
	// OWN scheme describes what IT emits, i.e. the NEXT (consuming) fragment's
	// INPUT partitioning -- never the skewed stage's own scheme, which
	// describes what it emits, not what it receives.
	PartitioningScheme *RawPartitioningScheme `json:"partitioningScheme,omitempty"`
}

// RawSourceLocation is a raw structured-plan node's {line, column} position
// in the original SQL text (round-5, §5.4.1 A3(iii)) — best-effort/optional
// everywhere it is read; engine/version-dependent.
type RawSourceLocation struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// RawVariable is a bare {name} reference from a raw structured-plan node
// (round-5 join-criteria resolution) — deliberately minimal: only the field
// this enrichment reads, never a full expression/type model.
type RawVariable struct {
	Name           string             `json:"name"`
	SourceLocation *RawSourceLocation `json:"sourceLocation,omitempty"`
}

// RawJoinCriterion is one equi-join predicate {left, right} from a raw
// JoinNode's criteria[] (round-5, §5.4.1 A3(iii)) — the primary source for a
// fan-out-chain row's joinKeys/sqlLine; plan_node.ParseJoins on the textual
// plan (already used by the NDV fingerprint) is the fallback when criteria[]
// itself is absent/empty.
type RawJoinCriterion struct {
	Left  RawVariable `json:"left"`
	Right RawVariable `json:"right"`
}

// RawConnectorHandle/RawTableHandle capture just enough of a TableScanNode's
// table handle to name its source table (round-5 build-table resolution) --
// deliberately minimal and absent-tolerant: an unrecognized connector shape
// simply leaves SchemaName/TableName empty (never an error), and the caller
// degrades to a fallback label.
type RawConnectorHandle struct {
	SchemaName string `json:"schemaName"`
	TableName  string `json:"tableName"`
}

type RawTableHandle struct {
	ConnectorHandle RawConnectorHandle `json:"connectorHandle"`
}

// RawPartitioningScheme captures just the outputLayout of an ExchangeNode's
// partitioningScheme (round-6, §5.4.1 A3(iii)(ζ) deadJoin resolution):
// ExchangeNode does not serialize a plain top-level `outputVariables` field
// the way most other node kinds do -- Presto's own
// ExchangeNode.getOutputVariables() returns exactly this outputLayout list,
// so it is the equivalent "what columns does this node emit" fact for an
// Exchange hop (the most common node shape immediately under a join's build
// side, verified on the real terminal).
type RawPartitioningScheme struct {
	OutputLayout []RawVariable `json:"outputLayout,omitempty"`
	// Partitioning (round-8, §5.4.5 R8-D): the actual hash-partitioning KEY
	// expression list -- distinct from OutputLayout (every column the
	// fragment emits): Partitioning.Arguments is specifically the column(s)
	// rows are partitioned BY, which is what R8-D's per-column skew
	// attribution needs (OutputLayout would include unrelated passthrough
	// columns too).
	Partitioning *RawPartitioning `json:"partitioning,omitempty"`
}

// RawPartitioning is a RawPartitioningScheme's nested {handle, arguments}
// object (round-8, §5.4.5 R8-D): only Arguments (the partition-key column
// list) is read; the connector handle itself is not modeled.
type RawPartitioning struct {
	Arguments []RawVariable `json:"arguments,omitempty"`
}

// RawPlanNode is a minimal, absent-tolerant view of ONE node in the raw
// structured plan (StagePlan.Root and its per-stage children; round-5,
// §5.4.1 A3(iii)/§10 item 14 -- "add minimal, absent-tolerant raw-plan
// structs... don't over-parse the plan"): only the fields the build-table /
// join-key / sourceLocation walk needs, covering every node shape the walk
// can encounter --
//   - JoinNode: Left/Right (probe/build sides) + Criteria
//   - single-child "pass-through" nodes (Project/Filter/TopN/Aggregation/
//     MarkDistinct/Output/etc.): Source
//   - multi-source nodes (Exchange/Union): Sources
//   - RemoteSourceNode (a cross-stage boundary leaf): SourceFragmentIds
//   - TableScanNode: Table
//
// This is deliberately NOT a fully typed plan-node hierarchy (real Presto has
// dozens of node kinds): unrecognized/irrelevant JSON fields are simply
// ignored by encoding/json, so parsing a plan shape this walk was not written
// for never errors -- the walk just stops there and the caller falls back to
// a label, exactly per the design's absent-tolerant contract.
type RawPlanNode struct {
	Type           string             `json:"@type"`
	Id             string             `json:"id"`
	SourceLocation *RawSourceLocation `json:"sourceLocation,omitempty"`

	// JoinNode: Kind carries the join's own "type" attribute ("LEFT"/"INNER"/
	// etc., round-6 §5.4.1 A3(iii)(ζ)) -- a generic field name deliberately
	// reused across node kinds (ExchangeNode's own "type", e.g. "REPARTITION"/
	// "GATHER", uses the same JSON key), since only JoinNode's value is ever
	// read by this package.
	Kind     string             `json:"type,omitempty"`
	Left     *RawPlanNode       `json:"left,omitempty"`
	Right    *RawPlanNode       `json:"right,omitempty"`
	Criteria []RawJoinCriterion `json:"criteria,omitempty"`

	// OutputVariables is the column set a node emits (round-6 deadJoin
	// resolution): present directly on most node kinds (JoinNode,
	// TableScanNode, RemoteSourceNode, Project/Filter/etc.); ExchangeNode
	// carries the equivalent list under PartitioningScheme.OutputLayout
	// instead (rawNodeOutputVarNames, chainenrich.go, checks both).
	OutputVariables    []RawVariable          `json:"outputVariables,omitempty"`
	PartitioningScheme *RawPartitioningScheme `json:"partitioningScheme,omitempty"`

	// Single-child pass-through nodes.
	Source *RawPlanNode `json:"source,omitempty"`

	// Multi-source nodes (e.g. ExchangeNode).
	Sources []*RawPlanNode `json:"sources,omitempty"`

	// RemoteSourceNode: the OTHER stage(s) this leaf reads from.
	SourceFragmentIds []string `json:"sourceFragmentIds,omitempty"`

	// TableScanNode.
	Table *RawTableHandle `json:"table,omitempty"`

	// AggregationNode (round-8, §5.4.1 A3(iii)(η) R8-A): Step is the
	// aggregation's own execution step ("SINGLE"/"PARTIAL"/"FINAL"/
	// "INTERMEDIATE") -- only SINGLE/PARTIAL steps are ever classified for
	// duplicate-sensitivity (a FINAL/INTERMEDIATE step consumes partial
	// aggregation STATE, not rows, per the design's review-C2 correction).
	// Aggregations is keyed by the aggregation's own output variable name
	// (e.g. "sum_123"); the map key itself is never read, only iterated --
	// callers sort by RawAggregation.Call.DisplayName for determinism (W5),
	// never by map iteration order.
	Step         string                    `json:"step,omitempty"`
	Aggregations map[string]RawAggregation `json:"aggregations,omitempty"`

	// MarkDistinctNode (round-8, §5.4.1 A3(iii)(η) R8-A): MarkerVariable is
	// the boolean marker column this node PRODUCES -- Presto lowers
	// COUNT(DISTINCT x) (and other DISTINCT aggregates) to
	// count(x) FILTER (WHERE markerVariable) elsewhere in the plan; a mask
	// variable tracing back to THIS field (never any other node's mask) is
	// the mandatory producer-trace proof of genuine DISTINCT qualification
	// (review C1) -- a structurally identical mask produced by
	// ImplementFilteredAggregations (a plain SQL FILTER) does NOT trace here
	// and must classify duplicate-SENSITIVE.
	MarkerVariable *RawVariable `json:"markerVariable,omitempty"`
}

// RawCall is a raw AggregationNode aggregation's function-call shape (round-8,
// §5.4.1 A3(iii)(η) R8-A): deliberately minimal -- only DisplayName (the
// function name used for allow-list/unknown-function classification) and
// Arguments (used by the W1 argument-lineage qualifier to resolve which side
// of the sole fanning join the aggregate's input originates from).
type RawCall struct {
	DisplayName string        `json:"displayName"`
	Arguments   []RawVariable `json:"arguments,omitempty"`
}

// RawAggregation is one entry of an AggregationNode's aggregations map
// (round-8, §5.4.1 A3(iii)(η) R8-A): Distinct is Presto's own `distinct==true`
// marker (rare in practice -- DISTINCT aggregates are normally lowered to a
// MarkDistinctNode + Mask instead, verified zero `distinct:true` occurrences
// across all three real fixtures); Mask is the boolean filter variable this
// aggregation applies (present for both MarkDistinct-lowered DISTINCT
// aggregates AND plain SQL FILTER (WHERE ...) clauses -- indistinguishable by
// shape alone, hence the mandatory MarkDistinctNode.MarkerVariable producer
// trace, review C1); Arguments/Call.Arguments both carry the aggregate's own
// argument list (kept on both for absent-tolerance across engine variants).
type RawAggregation struct {
	Call      RawCall       `json:"call"`
	Distinct  bool          `json:"distinct,omitempty"`
	Mask      *RawVariable  `json:"mask,omitempty"`
	Arguments []RawVariable `json:"arguments,omitempty"`
}

// rawNodeTypeName strips a raw plan node's `@type` down to its bare class
// name (e.g. ".JoinNode" or "com.facebook...plan.RemoteSourceNode" ->
// "JoinNode"/"RemoteSourceNode"), for both type-suffix matching and building
// a never-blank fallback label from an unresolvable node.
func rawNodeTypeName(t string) string {
	if idx := strings.LastIndexByte(t, '.'); idx >= 0 && idx+1 < len(t) {
		return t[idx+1:]
	}
	return t
}

// rawNodeIsType reports whether a raw plan node's `@type` names the given
// bare class (suffix match, tolerating both Presto's abbreviated ".Foo" form
// and the fully qualified "com.facebook...Foo" form).
func rawNodeIsType(node *RawPlanNode, bareName string) bool {
	return node != nil && rawNodeTypeName(node.Type) == bareName
}

// StageExecutionInfo carries the stage's state and per-task stats.
type StageExecutionInfo struct {
	State string      `json:"state"`
	Tasks []*TaskInfo `json:"tasks"`
}

// TaskInfo is one entry of a stage's tasks[] array.
type TaskInfo struct {
	TaskId     string      `json:"taskId"`
	TaskStatus *TaskStatus `json:"taskStatus"`
	Stats      *TaskStats  `json:"stats"`
}

// TaskStatus carries the task-level GC counters (verified present as numeric,
// never null, at this level — §2).
type TaskStatus struct {
	FullGcCount        *int64 `json:"fullGcCount"`
	FullGcTimeInMillis *int64 `json:"fullGcTimeInMillis"`
}

// TaskStats carries the numeric *InNanos/*InBytes task fields used for skew
// analysis (§2) — as opposed to queryStats' human-readable string aliases.
type TaskStats struct {
	TotalScheduledTimeInNanos int64  `json:"totalScheduledTimeInNanos"`
	TotalCpuTimeInNanos       int64  `json:"totalCpuTimeInNanos"`
	TotalBlockedTimeInNanos   int64  `json:"totalBlockedTimeInNanos"`
	RawInputDataSizeInBytes   int64  `json:"rawInputDataSizeInBytes"`
	RawInputPositions         int64  `json:"rawInputPositions"`
	ProcessedInputPositions   int64  `json:"processedInputPositions"`
	FullGcCount               *int64 `json:"fullGcCount"`
	FullGcTimeInMillis        *int64 `json:"fullGcTimeInMillis"`
}

// NumericStageId strips the "queryId." prefix from a stage id (matching
// queryjson.StageInfo.processForInsert's convention) and parses the numeric
// suffix. Returns ok=false when the id has no parseable numeric suffix.
func NumericStageId(stageId string) (int, bool) {
	s := stageId
	if idx := strings.LastIndexByte(s, '.'); idx >= 0 && idx+1 < len(s) {
		s = s[idx+1:]
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

// Flatten returns every stage in this subtree (this node first, then children
// depth-first), mirroring queryjson.StageInfo.PrepareForInsert's traversal order.
func (s *StageNode) Flatten() []*StageNode {
	if s == nil {
		return nil
	}
	out := []*StageNode{s}
	for _, c := range s.SubStages {
		out = append(out, c.Flatten()...)
	}
	return out
}

// PlanNodeIds returns the set of plan-node ids present in this stage's own plan
// fragment (not descending into remote-source children, which belong to other
// stages). Used to localize a runaway stage number to the plan nodes inside it
// (§5.6) without depending on operatorSummaries (which RUNNING native stages do
// not report).
func (s *StageNode) PlanNodeIds() map[string]bool {
	ids := map[string]bool{}
	if s == nil || s.Plan == nil || s.Plan.JsonRepresentation == "" {
		return ids
	}
	var root plan_node.PlanNode
	// See StripEstimates: real native-engine plan.jsonRepresentation carries a
	// string-enum "confident" inside estimates[], which is incompatible with
	// plan_node.PlanEstimate.Confident (bool) and would otherwise abort/truncate
	// the unmarshal deep in the tree (verified against real data). Diagnosis
	// does not need the embedded estimates (planStatsAndCosts is authoritative),
	// so they are stripped defensively before unmarshaling.
	if err := json.Unmarshal(StripEstimates([]byte(s.Plan.JsonRepresentation)), &root); err != nil {
		return ids
	}
	var walk func(n *plan_node.PlanNode)
	walk = func(n *plan_node.PlanNode) {
		if n == nil {
			return
		}
		if n.Name != plan_node.RemoteSource {
			ids[n.Id] = true
		}
		for i := range n.Children {
			walk(&n.Children[i])
		}
	}
	walk(&root)
	return ids
}
