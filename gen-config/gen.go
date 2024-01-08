package gen_config

import (
	"io/fs"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"text/template"
)

var fm = template.FuncMap{
	// decrement
	"dec": func(i uint) uint {
		return i - 1
	},
	"mul": func(a uint, b uint) uint { return a * b },
}

// GenerateFiles For each .tmpl file under the "template" directory, generate the corresponding file with the same
// directory structure for each config.
func GenerateFiles(configs []*ClusterConfig) {
	traverseTemplateDir := func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		tmpl, err := template.New(d.Name()).Funcs(fm).ParseFS(builtinTemplate, path)
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("failed to parse template")
			return nil
		}

		for _, cfg := range configs {
			outputPath, _ := filepath.Rel(filepath.Dir(path), path)
			outputPath = filepath.Join(cfg.Path, outputPath)
			f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Error().Err(err).Str("output_path", outputPath).Msg("failed to create file")
				return nil
			}
			err = tmpl.Execute(f, cfg)
			if err != nil {
				log.Error().Err(err).Str("output_path", outputPath).Msg("failed to evaluate template")
				return nil
			}
			log.Info().Msgf("wrote %s", outputPath)
		}
		return nil
	}
	if TemplateDir != "" {
		filepath.WalkDir(TemplateDir, traverseTemplateDir)
	} else {
		fs.WalkDir(builtinTemplate, ".", traverseTemplateDir)
	}

}
