package forward

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/utils"

	presto "github.com/ethanyzhang/presto-go"
	"github.com/ethanyzhang/presto-go/query_json"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var (
	DryRun                bool
	PrestoFlagsArray      utils.PrestoFlagsArray
	OutputPath            string
	RunName               string
	PollInterval          time.Duration
	ExcludePatternStrings []string
	ReplacePatternStrings []string
	SchemaMappingStrings  []string

	excludePatterns []*regexp.Regexp
	replacePatterns []*regexp.Regexp
	replaceStrings  []string
	schemaMappings  = make(map[string]string)
	runningTasks    sync.WaitGroup
	failedToForward atomic.Uint32
	forwarded       atomic.Uint32
	// runningQueriesCacheMap caches mapping for original queries to forwarded queries.
	// The key is the queryId in source cluster. The values are the queries running on target clusters,
	// it includes nextUri and the pointer to the target cluster client.
	runningQueriesCacheMap = make(map[string][]*QueryCacheEntry)
	queryCacheMutex        = &sync.RWMutex{}
)

const (
	maxRetry                 = 10
	queryStateErrorCancelled = "USER_CANCELED"
	queryStateFailed         = "FAILED"
)

type QueryCacheEntry struct {
	NextUri string
	Client  *presto.Client
}

func waitForNextPoll(ctx context.Context) {
	timer := time.NewTimer(PollInterval)
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

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

	// Compile the regular expressions to filter queries to forward.
	for i, excludePatternStr := range ExcludePatternStrings {
		if regex, err := regexp.Compile(excludePatternStr); err == nil {
			excludePatterns = append(excludePatterns, regex)
			log.Info().Str("pattern", excludePatternStr).Msg("added exclude pattern")
		} else {
			log.Warn().Str("pattern", excludePatternStr).Err(err).Msgf("failed to compile exclude pattern %d", i)
		}
	}

	// We take string pairs of (regular expression string, replace string) from this array.
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

	for i := 0; i+1 < len(SchemaMappingStrings); i += 2 {
		schemaMappings[SchemaMappingStrings[i]] = SchemaMappingStrings[i+1]
		log.Info().Msgf("added schema mapping from %s to %s", SchemaMappingStrings[i], SchemaMappingStrings[i+1])
	}

	sourceClient := clients[0]
	trueValue := true
	// We do not need query text from the queryState because we will need to query the detailed info for session params anyway.
	queryTextSizeLimit := 1
	// lastQueryStateCheckCutoffTime is the query create time of the most recent query in the previous batch.
	// We only look at queries created later than this timestamp in the following batch.
	lastQueryStateCheckCutoffTime := time.Time{}
	// GetQueryState API will always return the full query history for all queries that have not yet been expired by the
	// Presto server. This can be a huge list. We do not want to do forwarding for queries that were already submitted
	// before this program started. So we need to skip the forwarding for the API result we got in the first batch.
	firstBatch := true
	// Keep running until user interrupts or quits using Ctrl + C or Ctrl + D.
	// When the cluster is unavailable to return the running queries, wait and retry for at most 10 times before quitting.
	for attempt := 1; ctx.Err() == nil && attempt <= maxRetry; {
		states, _, err := sourceClient.GetQueryState(ctx, &presto.GetQueryStateOptions{
			IncludeAllQueries:  &trueValue,
			QueryTextSizeLimit: &queryTextSizeLimit,
		})
		if err != nil {
			log.Error().Err(err).Msgf("failed to get query states, attempt %d/%d", attempt, maxRetry)
			attempt++
			waitForNextPoll(ctx)
			continue
		} else {
			attempt = 1
		}
		// GetQueryState API does not return results (queries) in chronological order. Therefore, we cannot update
		// lastQueryStateCheckCutoffTime directly because we may update it to be too recent so some queries we do need
		// to process get filtered out.
		newCutoffTime := lastQueryStateCheckCutoffTime
		for _, state := range states {
			// Check if there is query in cancel status
			if state.QueryState == queryStateFailed && state.ErrorCode.Name == queryStateErrorCancelled {
				go checkAndCancelQuery(ctx, &state)
			}
			if !state.CreateTime.After(lastQueryStateCheckCutoffTime) {
				// We looked at this query in the previous batch.
				continue
			}
			if newCutoffTime.Before(state.CreateTime) {
				newCutoffTime = state.CreateTime
			}
			if !firstBatch {
				// As we mentioned above, we do not do forwarding for the first batch.
				runningTasks.Add(1)
				go forwardQuery(ctx, &state, clients)
			}
		}
		firstBatch = false
		lastQueryStateCheckCutoffTime = newCutoffTime
		waitForNextPoll(ctx)
	}
	if ctx.Err() == nil {
		// We exited the loop because the source server is not able to return the query states after maxRetry attempts.
		// Not because of user interruption.
		log.Error().Msgf("failed to get query state info from the source cluster after %d attempts", maxRetry)
	}
	runningTasks.Wait()
	// This causes the signal handler to exit.
	close(timeToExit)
	log.Info().Uint32("forwarded", forwarded.Load()).Uint32("failed_to_forward", failedToForward.Load()).
		Msgf("finished forwarding queries")
}

func checkAndCancelQuery(ctx context.Context, queryState *presto.QueryStateInfo) {
	queryCacheMutex.RLock()
	queryCacheEntries, ok := runningQueriesCacheMap[queryState.QueryId]
	queryCacheMutex.RUnlock()

	if ok {
		for _, q := range queryCacheEntries {
			if q.NextUri != "" {
				_, _, cancelQueryErr := q.Client.CancelQuery(ctx, q.NextUri)
				if cancelQueryErr != nil {
					log.Error().Msgf("cancel query failed on target cluter: %s error: %s", q.NextUri, cancelQueryErr.Error())
				}
			}
		}
	}
}

func forwardQuery(ctx context.Context, queryState *presto.QueryStateInfo, clients []*presto.Client) {
	defer runningTasks.Done()
	var queryInfoErr error
	queryInfo := new(query_json.QueryInfo)
	for attempt := 1; attempt <= maxRetry; attempt++ {
		_, queryInfoErr = clients[0].GetQueryInfo(ctx, queryState.QueryId, queryInfo)
		if queryInfoErr != nil {
			queryInfo = new(query_json.QueryInfo)
			log.Error().Str("source_query_id", queryState.QueryId).Err(queryInfoErr).
				Msgf("failed to get query info for forwarding, attempt %d/%d", attempt, maxRetry)
			waitForNextPoll(ctx)
		} else {
			break
		}
	}
	if queryInfoErr != nil {
		log.Error().Str("source_query_id", queryState.QueryId).
			Msgf("cannot get query info for forwarding after %d retries, skipping", maxRetry)
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
	if schema := queryInfo.Session.Schema; schema != nil {
		if mappedSchema, exists := schemaMappings[*schema]; exists {
			queryInfo.Session.Schema = &mappedSchema
			log.Info().Str("source_query_id", queryInfo.QueryId).
				Msgf("schema replaced %s -> %s", *schema, mappedSchema)
		}
	}
	SessionPropertyHeader := clients[0].GenerateSessionParamsHeaderValue(queryInfo.Session.CollectSessionProperties())
	if DryRun {
		logEntry := log.Info().Str("query", queryInfo.Query)
		if queryInfo.Session.Schema != nil {
			logEntry = logEntry.Str("schema", *queryInfo.Session.Schema)
		}
		logEntry.Msg("query not sent in dry-run mode")
		return
	}
	successful, failed := atomic.Uint32{}, atomic.Uint32{}
	forwardedQueries := sync.WaitGroup{}
	cachedQueries := make([]*QueryCacheEntry, len(clients)-1)
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
			//build cache for running query
			if clientResult.NextUri != nil {
				cachedQueries[i-1] = &QueryCacheEntry{NextUri: *clientResult.NextUri, Client: client}
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
	//Add running query into to cache
	queryCacheMutex.Lock()
	runningQueriesCacheMap[queryState.QueryId] = cachedQueries
	queryCacheMutex.Unlock()
	log.Debug().Msg("adding query to cache" + queryState.QueryId)
	forwardedQueries.Wait()
	log.Info().Str("source_query_id", queryInfo.QueryId).Uint32("successful", successful.Load()).
		Uint32("failed", failed.Load()).Msg("query forwarding finished")
	forwarded.Add(1)
	//remove finished query from cache
	queryCacheMutex.Lock()
	delete(runningQueriesCacheMap, queryState.QueryId)
	queryCacheMutex.Unlock()
	log.Info().Msg("removing query from cache" + queryState.QueryId)
}
