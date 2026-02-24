package clusters

import "embed"

var (
	//go:embed all:templates
	BuiltinTemplate embed.FS

	//go:embed params.json
	BuiltinGeneratorParametersBytes []byte
)
