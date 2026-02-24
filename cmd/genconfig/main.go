package genconfig

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"pbench/clusters"
	"pbench/log"

	"github.com/spf13/cobra"
)

const genconfigJson = "config.json"

var (
	TemplatePath   = ""
	TemplateFS     fs.FS
	ParameterPaths []string
)

func Run(_ *cobra.Command, args []string) {
	params := make(map[string]any)
	if len(ParameterPaths) > 0 {
		for _, paramPath := range ParameterPaths {
			paramBytes, ioErr := os.ReadFile(paramPath)
			if ioErr != nil {
				log.Error().Err(ioErr).Str("parameter_path", paramPath).
					Msg("failed to read generator parameter file")
				continue
			}
			m := make(map[string]any)
			if unmarshalErr := json.Unmarshal(paramBytes, &m); unmarshalErr != nil {
				log.Error().Err(unmarshalErr).Str("parameter_path", paramPath).
					Msg("failed to unmarshal generator parameter file")
				continue
			}
			for k, v := range m {
				params[k] = v
			}
		}
	} else {
		if unmarshalErr := json.Unmarshal(clusters.BuiltinGeneratorParametersBytes, &params); unmarshalErr != nil {
			log.Error().Err(unmarshalErr).Msg("failed to unmarshal built-in generator parameters")
			return
		}
	}
	if TemplatePath == "" {
		TemplateFS = clusters.BuiltinTemplate
		TemplatePath = "templates"
	} else {
		TemplateFS = os.DirFS(TemplatePath)
		TemplatePath = "."
	}
	configs := make([]ConfigData, 0, 3)
	_ = filepath.Walk(args[0], func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}
		if info.IsDir() || info.Name() != genconfigJson {
			return nil
		}
		bytes, ioErr := os.ReadFile(path)
		if ioErr != nil {
			log.Error().Err(ioErr).Str("path", path).Msg("failed to read config file.")
			return nil
		}
		configMap := make(map[string]any)
		if ioErr = json.Unmarshal(bytes, &configMap); ioErr != nil {
			log.Error().Err(ioErr).Str("path", path).Msg("failed to parse config file.")
			return nil
		}
		// Merge: start with params, overlay config values.
		merged := make(map[string]any, len(params)+len(configMap))
		for k, v := range params {
			merged[k] = v
		}
		for k, v := range configMap {
			merged[k] = v
		}
		log.Info().Str("path", path).Msg("parsed configuration")
		configs = append(configs, ConfigData{
			Path:   filepath.Dir(path),
			Values: merged,
		})
		return nil
	})
	// Generate config files for each config we read.
	GenerateFiles(configs)
}

func PrintDefaultParams(_ *cobra.Command, _ []string) {
	fmt.Print(string(clusters.BuiltinGeneratorParametersBytes))
}
