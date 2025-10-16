package stage

import (
	"fmt"
	"os"
	"path/filepath"
)

// Streams defines the configuration for stream-based execution
type Streams struct {
	StreamPath  string  `json:"stream_file_path"`
	StreamCount int     `json:"stream_count"`
	Seeds       []int64 `json:"seeds,omitempty"`
}

// Validate checks if the Streams configuration is valid
func (s *Streams) Validate() error {
	if s.StreamCount <= 0 {
		return fmt.Errorf("stream_count must be positive, got %d for stream %s", s.StreamCount, s.StreamPath)
	}

	if len(s.Seeds) > 0 {
		if len(s.Seeds) != 1 && len(s.Seeds) != s.StreamCount {
			return fmt.Errorf("seeds array length (%d) must be either 1 or equal to stream_count (%d) for stream %s",
				len(s.Seeds), s.StreamCount, s.StreamPath)
		}
	}

	return nil
}

// GetValidatedPath returns the absolute path to the stream file and validates it exists
func (s *Streams) GetValidatedPath(baseDir string) (string, error) {
	streamPath := s.StreamPath
	if !filepath.IsAbs(streamPath) {
		streamPath = filepath.Join(baseDir, streamPath)
	}

	if _, err := os.Stat(streamPath); err != nil {
		return "", fmt.Errorf("stream file %s does not exist: %w", streamPath, err)
	}

	return streamPath, nil
}

// GetSeedForInstance returns the appropriate seed for stream instance
func (s *Streams) GetSeedForInstance(instanceIndex int) (int64, bool) {
	if len(s.Seeds) == 0 {
		return 0, false
	}

	if len(s.Seeds) == 1 {
		return s.Seeds[0] + int64(instanceIndex), true
	}

	if instanceIndex <= len(s.Seeds) {
		return s.Seeds[instanceIndex], true
	}

	return 0, false
}
