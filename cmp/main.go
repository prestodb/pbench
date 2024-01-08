package cmp

import (
	"github.com/spf13/cobra"
	"regexp"
)

var (
	IdRegexStr string
	idRegex    *regexp.Regexp
)

func Run(_ *cobra.Command, args []string) {

}
