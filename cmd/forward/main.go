package forward

import (
	"context"
	"github.com/spf13/cobra"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/presto"
	"pbench/utils"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	PrestoFlagsArray      utils.PrestoFlagsArray
	OutputPath            string
	RunName               string
	PollInterval          time.Duration
	ExcludePatternStrings []string
	ReplacePatternStrings []string

	excludePatterns []*regexp.Regexp
	replacePatterns []*regexp.Regexp
	replaceStrings  []string
	runningTasks    sync.WaitGroup
	failedToForward atomic.Uint32
	forwarded       atomic.Uint32
)

func Run(_ *cobra.Command, _ []string) {
	OutputPath = filepath.Join(OutputPath, RunName)
	utils.PrepareOutputDirectory(OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(OutputPath, "forward.log")
	flushLog := utils.InitLogFile(logPath)
	defer flushLog()

	ctx, cancel := context.WithCancel(context.Background())
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	// Handle SIGINT, SIGTERM, and SIGQUIT. When ctx is canceled, in-progress MySQL transactions and InfluxDB operations will roll back.
	go func() {
		sig := <-timeToExit
		if sig != nil {
			log.Info().Msg("abort forwarding")
			cancel()
		}
	}()

	prestoClusters := PrestoFlagsArray.Pivot()
	// The design here is to forward the traffic from cluster 0 to the rest.
	sourceClusterSize := 0
	clients := make([]*presto.Client, 0, len(prestoClusters))
	for i, cluster := range prestoClusters {
		clients = append(clients, cluster.NewPrestoClient())
		// Check if we can connect to the cluster.
		if stats, _, err := clients[i].GetClusterInfo(ctx); err != nil {
			log.Fatal().Err(err).Msgf("cannot connect to cluster at position %d: %s", i, cluster.ServerUrl)
		} else if i == 0 {
			sourceClusterSize = stats.ActiveWorkers
		} else if stats.ActiveWorkers != sourceClusterSize {
			log.Warn().Msgf("the source cluster and target cluster %d do not match in size (%d != %d)", i, sourceClusterSize, stats.ActiveWorkers)
		}
	}

	for i, excludePatternStr := range ExcludePatternStrings {
		if regex, err := regexp.Compile(excludePatternStr); err == nil {
			excludePatterns = append(excludePatterns, regex)
			log.Info().Str("pattern", excludePatternStr).Msg("added exclude pattern")
		} else {
			log.Warn().Str("pattern", excludePatternStr).Err(err).Msgf("failed to compile exclude pattern %d", i)
		}
	}

	for i := 0; i+1 < len(ReplacePatternStrings); i += 2 {
		if regex, err := regexp.Compile(ReplacePatternStrings[i]); err == nil {
			replacePatterns = append(replacePatterns, regex)
			replaceStrings = append(replaceStrings, ReplacePatternStrings[i+1])
			log.Info().Str("pattern", ReplacePatternStrings[i]).Str("replace_with", ReplacePatternStrings[i+1]).
				Msg("added replace pattern")
		} else {
			log.Warn().Str("pattern", ReplacePatternStrings[i]).Err(err).Msgf("failed to compile replace pattern %d, skipping", i/2)
		}
	}

	sourceClient := clients[0]
	trueValue := true
	// lastQueryStateCheckCutoffTime is the query create time of the most recent query in the previous batch.
	// We only look at queries created later than this timestamp in the following batch.
	lastQueryStateCheckCutoffTime := time.Time{}
	firstBatch := true
	// Keep running until the source cluster becomes unavailable or the user interrupts or quits using Ctrl + C or Ctrl + D.
	for ctx.Err() == nil {
		states, _, err := sourceClient.GetQueryState(ctx, &presto.GetQueryStatsOptions{IncludeAllQueries: &trueValue})
		if err != nil {
			log.Error().Err(err).Msgf("failed to get query states")
			break
		}
		newCutoffTime := time.Time{}
		for _, state := range states {
			if !state.CreateTime.After(lastQueryStateCheckCutoffTime) {
				// We looked at this query in the previous batch.
				continue
			}
			if newCutoffTime.Before(state.CreateTime) {
				newCutoffTime = state.CreateTime
			}
			if !firstBatch {
				runningTasks.Add(1)
				go forwardQuery(ctx, &state, clients)
			}
		}
		firstBatch = false
		if newCutoffTime.After(lastQueryStateCheckCutoffTime) {
			lastQueryStateCheckCutoffTime = newCutoffTime
		}
		timer := time.NewTimer(PollInterval)
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
	runningTasks.Wait()
	// This causes the signal handler to exit.
	close(timeToExit)
	log.Info().Uint32("forwarded", forwarded.Load()).Uint32("failed_to_forward", failedToForward.Load()).
		Msgf("finished forwarding queries")
}

func forwardQuery(ctx context.Context, queryState *presto.QueryStateInfo, clients []*presto.Client) {
	defer runningTasks.Done()
	queryInfo, _, queryInfoErr := clients[0].GetQueryInfo(ctx, queryState.QueryId, false, nil)
	if queryInfoErr != nil {
		log.Error().Str("source_query_id", queryState.QueryId).Err(queryInfoErr).
			Msg("failed to get query info for forwarding")
		failedToForward.Add(1)
		return
	}
	for _, regex := range excludePatterns {
		if regex.MatchString(queryInfo.Query) {
			log.Info().Str("source_query_id", queryInfo.QueryId).
				Msgf("skipping query because it matches exclude pattern %s", regex.String())
			return
		}
	}
	for i, regex := range replacePatterns {
		replacedQuery := regex.ReplaceAllString(queryInfo.Query, replaceStrings[i])
		if queryInfo.Query != replacedQuery {
			log.Info().Str("source_query_id", queryInfo.QueryId).
				Msgf("replaced with pattern %s -> %s", regex.String(), replaceStrings[i])
			queryInfo.Query = replacedQuery
		}
	}
	SessionPropertyHeader := clients[0].GenerateSessionParamsHeaderValue(queryInfo.Session.CollectSessionProperties())
	successful, failed := atomic.Uint32{}, atomic.Uint32{}
	forwardedQueries := sync.WaitGroup{}
	for i := 1; i < len(clients); i++ {
		forwardedQueries.Add(1)
		go func(client *presto.Client) {
			defer forwardedQueries.Done()
			clientResult, _, queryErr := client.Query(ctx, queryInfo.Query, func(req *http.Request) {
				if queryInfo.Session.Catalog != nil {
					req.Header.Set(presto.CatalogHeader, *queryInfo.Session.Catalog)
				}
				if queryInfo.Session.Schema != nil {
					req.Header.Set(presto.SchemaHeader, *queryInfo.Session.Schema)
				}
				req.Header.Set(presto.SessionHeader, SessionPropertyHeader)
				req.Header.Set(presto.SourceHeader, queryInfo.QueryId)
			})
			if queryErr != nil {
				log.Error().Str("source_query_id", queryInfo.QueryId).
					Str("target_host", client.GetHost()).Err(queryErr).Msg("failed to execute query")
				failed.Add(1)
				return
			}
			rowCount := 0
			drainErr := clientResult.Drain(ctx, func(qr *presto.QueryResults) error {
				rowCount += len(qr.Data)
				return nil
			})
			if drainErr != nil {
				log.Error().Str("source_query_id", queryInfo.QueryId).
					Str("target_host", client.GetHost()).Err(drainErr).Msg("failed to fetch query result")
				failed.Add(1)
				return
			}
			successful.Add(1)
			log.Info().Str("source_query_id", queryInfo.QueryId).
				Str("target_host", client.GetHost()).Int("row_count", rowCount).Msg("query executed successfully")
		}(clients[i])
	}
	forwardedQueries.Wait()
	log.Info().Str("source_query_id", queryInfo.QueryId).Uint32("successful", successful.Load()).
		Uint32("failed", failed.Load()).Msg("query forwarding finished")
	forwarded.Add(1)
}
