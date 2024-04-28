package genconfig

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	cluster_configs "pbench/clusters"
	"pbench/log"

	"github.com/spf13/cobra"
)

const configJson = "config.json"

var (
	TemplatePath  = ""
	TemplateFS    fs.FS
	ParameterPath = ""
)

func Run(_ *cobra.Command, args []string) {
	gParams := DefaultGeneratorParameters
	if ParameterPath != "" {
		if paramsByte, ioErr := os.ReadFile(ParameterPath); ioErr != nil {
			log.Error().Err(ioErr).Str("parameter_path", ParameterPath).
				Msg("failed to read generator parameter file")
			ParameterPath = ""
		} else {
			params := &GeneratorParameters{}
			if unmarshalErr := json.Unmarshal(paramsByte, params); unmarshalErr != nil {
				log.Error().Err(unmarshalErr).Str("parameter_path", ParameterPath).
					Msg("failed to unmarshal generator parameter file")
			} else {
				gParams = params
			}
		}
	}
	if TemplatePath == "" {
		TemplateFS = cluster_configs.BuiltinTemplate
		TemplatePath = "templates"
	} else {
		TemplateFS = os.DirFS(TemplatePath)
		TemplatePath = "."
	}
	configs := make([]*ClusterConfig, 0, 3)
	_ = filepath.Walk(args[0], func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}
		if info.IsDir() || info.Name() != configJson {
			return nil
		}
		bytes, ioErr := os.ReadFile(path)
		if ioErr != nil {
			log.Error().Err(ioErr).Str("path", path).Msg("failed to read config file.")
			return nil
		}
		cfg := &ClusterConfig{
			GeneratorParameters: gParams,
		}
		if ioErr = json.Unmarshal(bytes, cfg); ioErr != nil {
			log.Error().Err(ioErr).Str("path", path).Msg("failed to parse config file.")
		} else {
			cfg.Path = filepath.Dir(path)
			// Calculate the variables based on the spec in the config.json
			log.Info().Str("path", path).Msg("parsed configuration")
			cfg.Calculate()
			configs = append(configs, cfg)
		}
		return nil
	})
	// Generate config files for each config we read.
	GenerateFiles(configs)
}

func PrintDefaultParams(_ *cobra.Command, _ []string) {
	fmt.Print(string(cluster_configs.BuiltinGeneratorParametersBytes))
}
