package gen_config

import (
	"io/fs"
	"os"
	"path/filepath"
	"pbench/log"
	"strings"
	"text/template"
)

var fm = template.FuncMap{
	// decrement
	"dec": func(i uint) uint {
		return i - 1
	},
	"mul": func(a uint, b uint) uint { return a * b },
	"add": func(a uint, b uint) uint { return a + b },
	"sub": func(a uint, b uint) uint { return a - b },
	"seq": func(start, end uint) (stream chan uint) {
		stream = make(chan uint)
		go func() {
			for i := start; i <= end; i++ {
				stream <- i
			}
			close(stream)
		}()
		return
	},
}

// GenerateFiles For each .tmpl file under the "template" directory, generate the corresponding file with the same
// directory structure for each config.
func GenerateFiles(configs []*ClusterConfig) {
	traverseTemplateDir := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", path).Send()
			// This is the only place where we return errors, in case the specified path is invalid,
			// we need to halt the processing.
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}

		tmpl, err := template.New(d.Name()).Funcs(fm).ParseFS(TemplateFS, path)
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("failed to parse template")
			return nil
		}

		for _, cfg := range configs {
			outputPath, _ := filepath.Rel(TemplatePath, path)
			outputPath = filepath.Join(cfg.Path, outputPath)
			err = os.MkdirAll(filepath.Dir(outputPath), 0755)
			if err != nil {
				log.Error().Err(err).Str("path", filepath.Dir(path)).Msg("failed to create directory")
				return nil
			}
			f, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Error().Err(err).Str("output_path", outputPath).Msg("failed to create file")
				continue
			}
			err = tmpl.Execute(f, cfg)
			if err != nil {
				log.Error().Err(err).Str("output_path", outputPath).Msg("failed to evaluate template")
				continue
			}
			log.Info().Msgf("wrote %s", outputPath)
		}
		return nil
	}
	_ = fs.WalkDir(TemplateFS, TemplatePath, traverseTemplateDir)
}
