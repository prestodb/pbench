package clusters

import "embed"

var (
	//go:embed templates
	BuiltinTemplate embed.FS

	//go:embed params.json
	BuiltinGeneratorParametersBytes []byte
)
