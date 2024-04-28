package run

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/presto"
	"pbench/stage"
	"pbench/utils"
	"strings"
	"time"
)

var (
	Name          string
	Comment       string
	PrestoFlags   utils.PrestoFlags
	RandSeed      int64
	RandSkip      int
	InfluxCfgPath string
	MySQLCfgPath  string
	PulumiCfgPath string
)

func Run(_ *cobra.Command, args []string) {
	parsedServerUrl, parseErr := url.Parse(PrestoFlags.ServerUrl)
	if parseErr != nil {
		log.Fatal().Err(parseErr).Str("server_url", PrestoFlags.ServerUrl).Msg("failed to parse server URL")
	}
	mainStage := &stage.Stage{
		States: &stage.SharedStageStates{
			RunName:      Name,
			Comment:      Comment,
			RandSeed:     RandSeed,
			RandSkip:     RandSkip,
			ServerFQDN:   parsedServerUrl.Host,
			RunStartTime: time.Now(),
			OutputPath:   PrestoFlags.OutputPath,
		},
	}

	var defaultRunNameBuilder *strings.Builder
	if mainStage.States.RunName == "" {
		defaultRunNameBuilder = &strings.Builder{}
	} else {
		mainStage.States.RunName = strings.ReplaceAll(mainStage.States.RunName, `%t`, mainStage.States.RunStartTime.Format(stage.RunNameTimeFormat))
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
		}
	}
	if defaultRunNameBuilder != nil {
		defaultRunNameBuilder.WriteByte('_')
		defaultRunNameBuilder.WriteString(mainStage.States.RunStartTime.Format(stage.RunNameTimeFormat))
		mainStage.States.RunName = defaultRunNameBuilder.String()
	}
	log.Info().Str("run_name", mainStage.States.RunName).Send()

	_, _, err := stage.ParseStageGraph(mainStage)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark stage graph")
	}

	if PrestoFlags.UserName != "" {
		if PrestoFlags.Password != "" {
			mainStage.States.GetClient = func() *presto.Client {
				client, _ := presto.NewClient(PrestoFlags.ServerUrl, PrestoFlags.IsTrino)
				client.UserPassword(PrestoFlags.UserName, PrestoFlags.Password)
				if PrestoFlags.ForceHttps {
					client.ForceHttps()
				}
				return client
			}
		} else {
			mainStage.States.GetClient = func() *presto.Client {
				client, _ := presto.NewClient(PrestoFlags.ServerUrl, PrestoFlags.IsTrino)
				client.User(PrestoFlags.UserName)
				if PrestoFlags.ForceHttps {
					client.ForceHttps()
				}
				return client
			}
		}
	} else {
		mainStage.States.GetClient = func() *presto.Client {
			client, _ := presto.NewClient(PrestoFlags.ServerUrl, PrestoFlags.IsTrino)
			if PrestoFlags.ForceHttps {
				client.ForceHttps()
			}
			return client
		}
	}
	mainStage.States.RegisterRunRecorder(stage.NewFileBasedRunRecorder())
	mainStage.States.RegisterRunRecorder(stage.NewInfluxRunRecorder(InfluxCfgPath))
	mySQLRunRecorder := stage.NewMySQLRunRecorder(MySQLCfgPath)
	mainStage.States.RegisterRunRecorder(mySQLRunRecorder)
	mainStage.States.RegisterRunRecorder(stage.NewPulumiMySQLRunRecorder(PulumiCfgPath, mySQLRunRecorder))
	os.Exit(mainStage.Run(context.Background()))
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
			newStage, err := processStagePath(fullPath)
			if err == nil {
				st.MergeWith(newStage)
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
