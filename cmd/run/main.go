package run

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/stage"
	"pbench/utils"
	"strings"
	"time"
)

var (
	Name          string
	Comment       string
	PrestoFlags   utils.PrestoFlags
	OutputPath    string
	RandSeed      int64
	RandSkip      int
	InfluxCfgPath string
	MySQLCfgPath  string
	PulumiCfgPath string
	// Snapshots and friends are the run-wide defaults for periodic query JSON snapshot
	// collection (diagnostic mode; see stage/json_snapshot.go). Individual stages can
	// override them via save_json_snapshots / json_snapshot_interval / json_snapshot_max /
	// json_snapshot_fetch_timeout.
	Snapshots            bool
	SnapshotInterval     time.Duration
	SnapshotMax          int
	SnapshotFetchTimeout time.Duration
)

func Run(_ *cobra.Command, args []string) {
	parsedServerUrl, parseErr := url.Parse(PrestoFlags.ServerUrl)
	if parseErr != nil {
		log.Fatal().Err(parseErr).Str("server_url", PrestoFlags.ServerUrl).Msg("failed to parse server URL")
	}
	utils.ExpandHomeDirectory(&OutputPath)
	utils.ExpandHomeDirectory(&InfluxCfgPath)
	utils.ExpandHomeDirectory(&MySQLCfgPath)
	utils.ExpandHomeDirectory(&PulumiCfgPath)
	mainStage := &stage.Stage{States: newSharedStageStates(parsedServerUrl)}

	var defaultRunNameBuilder *strings.Builder
	if mainStage.States.RunName == "" {
		defaultRunNameBuilder = &strings.Builder{}
	} else {
		mainStage.States.RunName = strings.ReplaceAll(mainStage.States.RunName, `%t`, mainStage.States.RunStartTime.Format(utils.DirectoryNameTimeFormat))
	}
	for _, path := range args {
		if st, err := processStagePath(path); err == nil {
			mainStage.MergeWith(st)
			if defaultRunNameBuilder != nil {
				if defaultRunNameBuilder.Len() > 0 {
					defaultRunNameBuilder.WriteByte('_')
				}
				defaultRunNameBuilder.WriteString(st.Id)
			}
		} else {
			os.Exit(1)
		}
	}
	if defaultRunNameBuilder != nil {
		defaultRunNameBuilder.WriteByte('_')
		defaultRunNameBuilder.WriteString(mainStage.States.RunStartTime.Format(utils.DirectoryNameTimeFormat))
		mainStage.States.RunName = defaultRunNameBuilder.String()
	}
	log.Info().Str("run_name", mainStage.States.RunName).Send()

	if _, _, err := stage.ParseStageGraph(mainStage); err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark stage graph")
	}

	mainStage.States.NewClient = PrestoFlags.NewPrestoClient
	mainStage.States.RegisterRunRecorder(stage.NewFileBasedRunRecorder())
	mainStage.States.RegisterRunRecorder(stage.NewInfluxRunRecorder(InfluxCfgPath))
	mySQLRunRecorder := stage.NewMySQLRunRecorder(MySQLCfgPath)
	mainStage.States.RegisterRunRecorder(mySQLRunRecorder)
	mainStage.States.RegisterRunRecorder(stage.NewPulumiMySQLRunRecorder(PulumiCfgPath, mySQLRunRecorder))
	os.Exit(mainStage.Run(context.Background()))
}

// newSharedStageStates builds the SharedStageStates populated from the run-level CLI flag
// vars, including the four --snapshots/--snapshot-interval/--snapshot-max/
// --snapshot-fetch-timeout vars (M2). Extracted out of Run() - which calls os.Exit() and so
// cannot be safely invoked from a test - specifically so this flag-to-SharedStageStates
// wiring can be asserted directly; see main_test.go.
func newSharedStageStates(parsedServerUrl *url.URL) *stage.SharedStageStates {
	return &stage.SharedStageStates{
		RunName:              Name,
		Comment:              Comment,
		RandSeed:             RandSeed,
		RandSkip:             RandSkip,
		ServerFQDN:           parsedServerUrl.Host,
		RunStartTime:         time.Now(),
		OutputPath:           OutputPath,
		SnapshotsEnabled:     Snapshots,
		SnapshotInterval:     SnapshotInterval,
		SnapshotMax:          SnapshotMax,
		SnapshotFetchTimeout: SnapshotFetchTimeout,
	}
}

func processStagePath(path string) (st *stage.Stage, returnErr error) {
	defer func() {
		if returnErr != nil {
			log.Error().Err(returnErr).Str("path", path).Msg("failed to process stage path")
		}
	}()
	stat, statErr := os.Stat(path)
	if statErr != nil {
		return nil, statErr
	}
	if stat.IsDir() {
		st = new(stage.Stage)
		entries, ioErr := os.ReadDir(path)
		if ioErr != nil {
			return nil, ioErr
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), stage.DefaultStageFileExt) {
				continue
			}
			fullPath := filepath.Join(path, entry.Name())
			if newStage, err := processStagePath(fullPath); err == nil {
				st.MergeWith(newStage)
			} else {
				return nil, err
			}
		}
		return st, nil
	} else {
		if !strings.HasSuffix(path, stage.DefaultStageFileExt) {
			return nil, fmt.Errorf("%s is not a %s file", path, stage.DefaultStageFileExt)
		}
		return stage.ReadStageFromFile(path)
	}
}
