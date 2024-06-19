package forward

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"pbench/log"
	"pbench/presto"
	"pbench/utils"
	"sync"
	"time"
)

var (
	PrestoFlagsArray utils.PrestoFlagsArray
	OutputPath       string
	RunName          string
	PollInterval     time.Duration

	runningTasks sync.WaitGroup
)

type QueryHistory struct {
	QueryId string     `presto:"query_id"`
	Query   string     `presto:"query"`
	Created *time.Time `presto:"created"`
}

func Run(_ *cobra.Command, _ []string) {
	//OutputPath = filepath.Join(OutputPath, RunName)
	//utils.PrepareOutputDirectory(OutputPath)
	//
	//// also start to write logs to the output directory from this point on.
	//logPath := filepath.Join(OutputPath, "forward.log")
	//flushLog := utils.InitLogFile(logPath)
	//defer flushLog()

	prestoClusters := PrestoFlagsArray.Assemble()
	// The design here is to forward the traffic from cluster 0 to the rest.
	sourceClusterSize := 0
	clients := make([]*presto.Client, 0, len(prestoClusters))
	for i, cluster := range prestoClusters {
		clients = append(clients, cluster.NewPrestoClient())
		if stats, _, err := clients[i].GetClusterInfo(context.Background()); err != nil {
			log.Fatal().Err(err).Msgf("cannot connect to cluster at position %d", i)
		} else if i == 0 {
			sourceClusterSize = stats.ActiveWorkers
		} else if stats.ActiveWorkers != sourceClusterSize {
			log.Warn().Msgf("source cluster size does not match target cluster %d size (%d != %d)", i, stats.ActiveWorkers, sourceClusterSize)
		}
	}

	sourceClient := clients[0]
	trueValue := true
	states, _, err := sourceClient.GetQueryState(context.Background(), &presto.GetQueryStatsOptions{
		IncludeAllQueries:            &trueValue,
		IncludeAllQueryProgressStats: nil,
		ExcludeResourceGroupPathInfo: nil,
		QueryTextSizeLimit:           nil,
	})
	if err != nil {
		log.Fatal().Err(err).Msgf("cannot get query states")
	}
	fmt.Printf("%#v", states)
}
