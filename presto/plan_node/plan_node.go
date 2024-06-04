package plan_node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

type PlanNodeTraverseMode uint8

const (
	PlanTreeBFSTraverse PlanNodeTraverseMode = iota
	PlanTreeDFSTraverse
)

const (
	RemoteSource = "RemoteSource"
	LeftJoin     = "LeftJoin"
	RightJoin    = "RightJoin"
	InnerJoin    = "InnerJoin"
)

var (
	nodeDepthCtxKey              = struct{}{}
	NoRootPlanNodeError          = errors.New("no root plan node found")
	NonExistentRemoteSourceError = errors.New("non-existent remote source")
	IsJoin                       = map[string]bool{
		LeftJoin:  true,
		RightJoin: true,
		InnerJoin: true,
	}
)

type PlanNodeTraverseFunction func(ctx context.Context, node *PlanNode) error

type PlanNode struct {
	Id            string         `json:"id"`
	Name          string         `json:"name"`
	Identifier    string         `json:"identifier"`
	Details       string         `json:"details"`
	Children      []PlanNode     `json:"children"`
	RemoteSources []string       `json:"remoteSources"`
	Estimates     []PlanEstimate `json:"estimates"`
}

func (n *PlanNode) GetTraverseDepth(ctx context.Context) int {
	depth, ok := ctx.Value(nodeDepthCtxKey).(int)
	if !ok {
		return -1
	}
	return depth
}

func (n *PlanNode) incrementTraverseDepth(ctx context.Context) context.Context {
	return context.WithValue(ctx, nodeDepthCtxKey, n.GetTraverseDepth(ctx)+1)
}

func (n *PlanNode) Traverse(ctx context.Context, fn PlanNodeTraverseFunction, planTree PlanTree, mode ...PlanNodeTraverseMode) error {
	traverseMode := PlanTreeBFSTraverse
	if len(mode) > 0 {
		traverseMode = mode[0]
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	visitChild := func() error {
		childCtx := n.incrementTraverseDepth(ctx)
		for _, child := range n.Children {
			if err := child.Traverse(childCtx, fn, planTree, mode...); err != nil {
				return err
			}
		}
		return nil
	}
	if traverseMode == PlanTreeDFSTraverse {
		if err := visitChild(); err != nil {
			return err
		}
	}
	if n.Name == RemoteSource && planTree != nil {
		for _, remoteSourceId := range n.RemoteSources {
			if remoteSource, exists := planTree[remoteSourceId]; exists {
				if err := remoteSource.Plan.Traverse(ctx, fn, planTree, mode...); err != nil {
					return err
				}
			} else {
				return fmt.Errorf("%w %s", NonExistentRemoteSourceError, remoteSourceId)
			}
		}
	} else if err := fn(ctx, n); err != nil {
		return err
	}
	if traverseMode == PlanTreeBFSTraverse {
		return visitChild()
	}
	return nil
}

type PlanTree map[string]*struct {
	Plan PlanNode `json:"plan"`
}

func (t PlanTree) Traverse(ctx context.Context, fn PlanNodeTraverseFunction, mode ...PlanNodeTraverseMode) error {
	if node, exists := t["0"]; exists {
		return node.Plan.Traverse(node.Plan.incrementTraverseDepth(ctx), fn, t, mode...)
	}
	return NoRootPlanNodeError
}

type PlanEstimate struct {
	OutputRowCount     JsonFloat64                   `json:"outputRowCount"`
	TotalSize          JsonFloat64                   `json:"totalSize"`
	Confident          bool                          `json:"confident"`
	VariableStatistics map[string]VariableStatistics `json:"variableStatistics"`
}

type VariableStatistics struct {
	LowValue            JsonFloat64 `json:"lowValue"`
	HighValue           JsonFloat64 `json:"highValue"`
	NullsFraction       JsonFloat64 `json:"nullsFraction"`
	AverageRowSize      JsonFloat64 `json:"averageRowSize"`
	DistinctValuesCount JsonFloat64 `json:"distinctValuesCount"`
}

type JsonFloat64 float64

func (f *JsonFloat64) MarshalJSON() ([]byte, error) {
	value := float64(*f)
	if math.IsNaN(value) {
		return []byte(`"NaN"`), nil
	} else if math.IsInf(value, 0) {
		if math.IsInf(value, -1) {
			return []byte(`"-Infinity"`), nil
		}
		return []byte(`"Infinity"`), nil
	} else {
		return json.Marshal(value)
	}
}

func (f *JsonFloat64) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*f = JsonFloat64(value)
		return nil
	case string:
		switch value {
		case "NaN":
			*f = JsonFloat64(math.NaN())
		case "Infinity":
			*f = JsonFloat64(math.Inf(1))
		case "-Infinity":
			*f = JsonFloat64(math.Inf(-1))
		default:
			return fmt.Errorf("invalid JsonFloat64 %s", value)
		}
		return nil
	default:
		return fmt.Errorf("invalid JsonFloat64")
	}
}
