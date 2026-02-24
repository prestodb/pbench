package genconfig

import (
	"bytes"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"pbench/log"
	"strings"
	"text/template"
)

// toFloat converts any numeric type to float64.
func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	default:
		return 0
	}
}

var fm = template.FuncMap{
	"dec": func(v any) int {
		return int(toFloat(v)) - 1
	},
	"mul": func(args ...any) float64 {
		result := 1.0
		for _, arg := range args {
			result *= toFloat(arg)
		}
		return result
	},
	"add": func(a, b any) float64 {
		return toFloat(a) + toFloat(b)
	},
	"sub": func(a, b any) float64 {
		return toFloat(a) - toFloat(b)
	},
	"div": func(a, b any) float64 {
		return toFloat(a) / toFloat(b)
	},
	"min": func(a, b any) float64 {
		return math.Min(toFloat(a), toFloat(b))
	},
	"max": func(a, b any) float64 {
		return math.Max(toFloat(a), toFloat(b))
	},
	"floor": func(v any) int {
		return int(math.Floor(toFloat(v)))
	},
	"ceil": func(v any) int {
		return int(math.Ceil(toFloat(v)))
	},
	"set": func(m map[string]any, key string, value any) string {
		m[key] = value
		return ""
	},
	"default": func(val, fallback any) any {
		if val == nil {
			return fallback
		}
		return val
	},
	"hasPrefix": func(s, prefix string) bool {
		return strings.HasPrefix(s, prefix)
	},
	"hasSuffix": func(s, suffix string) bool {
		return strings.HasSuffix(s, suffix)
	},
	"contains": func(s, substr string) bool {
		return strings.Contains(s, substr)
	},
	"seq": func(startAny, endAny any) (stream chan int) {
		start := int(toFloat(startAny))
		end := int(toFloat(endAny))
		if end < start {
			stream = make(chan int)
			close(stream)
			return
		}
		n := end - start + 1
		stream = make(chan int, n)
		for i := start; i <= end; i++ {
			stream <- i
		}
		close(stream)
		return
	},
}

// ConfigData holds the path and merged parameter values for a cluster configuration.
type ConfigData struct {
	Path   string
	Values map[string]any
}

const preludeFile = ".prelude"

// GenerateFiles generates configuration files from templates for each config.
// For each template file under the template directory, the corresponding file is generated
// with the same directory structure for each config. After generation, any files in the output
// directories that were not generated (and are not genconfig.json) are removed.
func GenerateFiles(configs []ConfigData) {
	// Try to read the prelude template.
	var preludeContent string
	if data, err := fs.ReadFile(TemplateFS, filepath.Join(TemplatePath, preludeFile)); err == nil {
		preludeContent = string(data)
	}

	// Track which files were written per config path.
	writtenFiles := make(map[string]map[string]bool, len(configs))
	for _, cfg := range configs {
		writtenFiles[cfg.Path] = make(map[string]bool)
	}

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

		tmpl := template.New(d.Name()).Funcs(fm)
		if preludeContent != "" {
			if _, parseErr := tmpl.Parse(preludeContent); parseErr != nil {
				log.Error().Err(parseErr).Msg("failed to parse prelude template")
				return nil
			}
		}
		if _, parseErr := tmpl.ParseFS(TemplateFS, path); parseErr != nil {
			log.Error().Err(parseErr).Str("path", path).Msg("failed to parse template")
			return nil
		}

		for _, cfg := range configs {
			outputPath, _ := filepath.Rel(TemplatePath, path)
			outputPath = filepath.Join(cfg.Path, outputPath)

			// Execute template to a buffer first to check for empty output.
			var buf bytes.Buffer
			if execErr := tmpl.Execute(&buf, cfg.Values); execErr != nil {
				log.Error().Err(execErr).Str("output_path", outputPath).Msg("failed to evaluate template")
				continue
			}
			if strings.TrimSpace(buf.String()) == "" {
				continue
			}

			err = os.MkdirAll(filepath.Dir(outputPath), 0755)
			if err != nil {
				log.Error().Err(err).Str("path", filepath.Dir(path)).Msg("failed to create directory")
				return nil
			}
			if err = os.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
				log.Error().Err(err).Str("output_path", outputPath).Msg("failed to write file")
				continue
			}
			writtenFiles[cfg.Path][outputPath] = true
			log.Info().Msgf("wrote %s", outputPath)
		}
		return nil
	}
	_ = fs.WalkDir(TemplateFS, TemplatePath, traverseTemplateDir)

	// Remove stale files that exist in output directories but have no corresponding template.
	for _, cfg := range configs {
		written := writtenFiles[cfg.Path]
		_ = filepath.Walk(cfg.Path, func(path string, info fs.FileInfo, err error) error {
			if err != nil || info.IsDir() || info.Name() == genconfigJson {
				return nil
			}
			if !written[path] {
				log.Info().Msgf("removing stale file %s", path)
				os.Remove(path)
			}
			return nil
		})
	}
}
