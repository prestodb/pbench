package run

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"net/url"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"presto-benchmark/stage"
	"strings"
	"time"
)

var (
	Name          string
	ServerUrl     = stage.DefaultServerUrl
	OutputPath    string
	UserName      string
	Password      string
	InfluxCfgPath string
	MySQLCfgPath  string
	PulumiCfgPath string
)

func Run(_ *cobra.Command, args []string) {
	parsedServerUrl, parseErr := url.Parse(ServerUrl)
	if parseErr != nil {
		log.Fatal().Err(parseErr).Str("server_url", ServerUrl).Msg("failed to parse server URL")
	}
	mainStage := &stage.Stage{
		States: &stage.SharedStageStates{
			RunName:      Name,
			ServerFQDN:   parsedServerUrl.Host,
			RunStartTime: time.Now(),
			OutputPath:   OutputPath,
		},
	}

	var defaultRunNameBuilder *strings.Builder
	if mainStage.States.RunName == "" {
		defaultRunNameBuilder = &strings.Builder{}
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

	if UserName != "" {
		if Password != "" {
			mainStage.States.GetClient = func() *presto.Client {
				client, _ := presto.NewClient(ServerUrl)
				client.UserPassword(UserName, Password)
				return client
			}
		} else {
			mainStage.States.GetClient = func() *presto.Client {
				client, _ := presto.NewClient(ServerUrl)
				client.User(UserName)
				return client
			}
		}
	} else {
		mainStage.States.GetClient = func() *presto.Client {
			client, _ := presto.NewClient(ServerUrl)
			return client
		}
	}
	mainStage.States.RegisterRunRecorder(stage.NewFileBasedRunRecorder())
	mainStage.States.RegisterRunRecorder(stage.NewInfluxRunRecorder(InfluxCfgPath))
	mySQLRunRecorder := stage.NewMySQLRunRecorder(MySQLCfgPath)
	mainStage.States.RegisterRunRecorder(mySQLRunRecorder)
	mainStage.States.RegisterRunRecorder(stage.NewPulumiMySQLRunRecorder(PulumiCfgPath, mySQLRunRecorder))
	mainStage.Run(context.Background())
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
