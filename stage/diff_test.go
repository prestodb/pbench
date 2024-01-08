package stage_test

import (
	"fmt"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"testing"
)

func TestTextDiff(t *testing.T) {
	aString := "\na\nbd\nc"
	bString := "a\nb\n"

	edits := myers.ComputeEdits(span.URIFromPath("a.txt1"), aString, bString)
	fmt.Println(gotextdiff.ToUnified("a.txt", "b.txt", aString, edits))
}
