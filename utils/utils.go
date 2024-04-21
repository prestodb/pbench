package utils

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"pbench/log"
)

func PrepareOutputDirectory(path string) {
	if stat, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, unix.ENOENT) {
			if mkdirErr := os.MkdirAll(path, 0755); mkdirErr != nil {
				log.Fatal().Err(mkdirErr).Msg("failed to create output directory")
			} else {
				log.Info().Str("output_path", path).Msg("output directory created")
			}
		} else {
			log.Fatal().Err(statErr).Msg("output path not valid")
		}
	} else if !stat.IsDir() {
		log.Fatal().Str("output_path", path).Msg("output path is not a directory")
	} else {
		log.Info().Str("output_path", path).Msg("output directory")
	}
}
