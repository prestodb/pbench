package gen_config

import (
	"embed"
	"encoding/json"
	"github.com/spf13/cobra"
	"io/fs"
	"os"
	"path/filepath"
	"presto-benchmark/log"
)

const configJson = "config.json"

var (
	TemplateDir   = ""
	ParameterPath = "params.json"
	//go:embed template
	builtinTemplate embed.FS
)

func Run(_ *cobra.Command, args []string) {
	gParams := DefaultGenerationParameters
	if ParameterPath != "" {
		if paramsByte, ioErr := os.ReadFile(ParameterPath); ioErr != nil {
			log.Error().Err(ioErr).Msg("failed to read generation parameter file")
			ParameterPath = ""
		} else {
			params := &GenerationParameters{}
			if unmarshalErr := json.Unmarshal(paramsByte, params); unmarshalErr != nil {
				gParams = params
			} else {
				log.Error().Err(unmarshalErr).Msg("failed to unmarshal generation parameter file")
			}
		}
	}
	configs := make([]*ClusterConfig, 0, 3)
	configPath, pathErr := filepath.Abs(args[0])
	if pathErr != nil {
		log.Error().Err(pathErr).Str("path", args[0]).Send()
		return
	}
	_ = filepath.Walk(configPath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || info.Name() != configJson {
			return nil
		}
		bytes, ioErr := os.ReadFile(path)
		if ioErr != nil {
			log.Error().Err(ioErr).Str("path", path).Msg("failed to read config file.")
			return nil
		}
		cfg := &ClusterConfig{
			GenerationParameters: gParams,
		}
		// If there is a config.json file under the subdirectory, parse it.
		if ioErr = json.Unmarshal(bytes, cfg); ioErr != nil {
			log.Error().Err(ioErr).Str("path", path).Msg("failed to parse config file.")
		} else {
			// Calculate the variables based on the spec in the config.json
			cfg.Calculate()
			cfg.Path = filepath.Dir(path)
			configs = append(configs, cfg)
		}
		return nil
	})
	// Generate config files for each config we read.
	GenerateFiles(configs)
}
