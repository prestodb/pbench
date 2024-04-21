package stage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/presto"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type Stage struct {
	// Id is used to uniquely identify a stage. It is usually the file name without its directory path and extension.
	Id string `json:"-"`
	// The values in Catalog, Schema, and SessionParams are inherited by the descendant stages. Please note that if
	// they have new values assigned in a stage, those values are NOT applied tn the Presto client until a stage
	// creates its own client by setting StartOnNewClient = true.
	Catalog       *string        `json:"catalog,omitempty"`
	Schema        *string        `json:"schema,omitempty"`
	SessionParams map[string]any `json:"session_params,omitempty"`
	TimeZone      *string        `json:"timezone"`
	Queries       []string       `json:"queries,omitempty"`
	// If a stage has both Queries and QueryFiles, the queries in the Queries array will be executed first then
	// the QueryFiles will be read and executed.
	QueryFiles []string `json:"query_files,omitempty"`
	// A map from [catalog.schema] to arrays of integers as expected row counts for all the queries we run
	// under different schemas. This includes the queries from both Queries and QueryFiles. Queries first and QueryFiles follows.
	// Can use regexp as key to match multiple [catalog.schema] pairs.
	ExpectedRowCounts map[string][]int `json:"expected_row_counts"`
	// When RandomExecution is turned on, we randomly pick queries to run until a certain number of queries/a specific
	// duration has passed. Expected row counts will not be checked in this mode because we cannot figure out the correct
	// expected row count offset.
	RandomExecution bool `json:"random_execution,omitempty"`
	// Use RandomlyExecuteUntil to specify a duration like "1h" or an integer as the number of queries should be executed
	// before exiting.
	RandomlyExecuteUntil string `json:"randomly_execute_until,omitempty"`
	// If not set, the default is 1. The default value is set when the stage is run.
	ColdRuns int `json:"cold_runs,omitempty"`
	// If not set, the default is 0.
	WarmRuns int `json:"warm_runs,omitempty"`
	// If StartOnNewClient is set to true, this stage will create a new client to execute itself.
	// This new client will be passed down to its descendant stages unless those stages also set StartOnNewClient to true.
	// Each client can carry their own set of client information, tags, session properties, user credentials, etc.
	// Descendant stages will **NOT** inherit this value from their parents so this is declared as a value not a pointer.
	StartOnNewClient bool `json:"start_on_new_client,omitempty"`
	// If AbortOnError is set to true, the context associated with this stage will be canceled if an error occurs.
	// Depending on when the cancellable context was created, this may abort some or all other running stages and all future stages.
	// Children stages will inherit this value from their parent if it is not set (nil).
	AbortOnError *bool `json:"abort_on_error,omitempty"`
	// If SaveOutput is set to true, the query result will be saved to files in its raw form.
	// Children stages will inherit this value from their parent if it is not set.
	// Cold runs will ignore this setting.
	SaveOutput *bool `json:"save_output,omitempty"`
	// If SaveColumnMetadata is set to true, we will save a json file of the query result's column metadata.
	// See the "columns" field in Presto's query API response.
	// Children stages will inherit this value from their parent if it is not set.
	// Cold runs will ignore this setting.
	SaveColumnMetadata *bool `json:"save_column_metadata,omitempty"`
	// If SaveJson is set to true, the query json will be saved to files in its raw form after the query is executed.
	// Children stages will inherit this value from their parent if it is not set.
	// When a query failed to execute for whatever reason, a query json file will be automatically saved even if this
	// knob was not set to true.
	SaveJson       *bool    `json:"save_json,omitempty"`
	NextStagePaths []string `json:"next,omitempty"`

	// BaseDir is set to the directory path of this stage's location. It is used to locate the descendant stages when
	// their locations are specified using relative paths. It is not possible to set this in a stage definition json file.
	// See ReadStageFromFile() - where BaseDir is set.
	BaseDir string `json:"-"`
	// States is a collection of **immutable** stage states that is shared by all stages linked in the main stage.
	// Some fields are inherited by the children stages but since they can be overwritten by the children stages too, so
	// they are not included here.
	States *SharedStageStates `json:"-"`
	// NextStages are populated via ParseStage.
	NextStages []*Stage `json:"-"`
	// Client is by default passed down to descendant stages.
	Client *presto.Client `json:"-"`

	// Convenient access to the expected row count array under the current schema.
	expectedRowCountInCurrentSchema []int
	// Convenient access to the catalog, schema, and timezone
	currentCatalog  string
	currentSchema   string
	currentTimeZone string
	// wgPrerequisites is a count-down latch to wait for all the prerequisites to finish before starting this stage.
	wgPrerequisites sync.WaitGroup

	// When a stage has multiple prerequisites, it will be called when each prerequisite stage finishes. started is here
	// to make sure this stage only starts once. When the stage is started by its first completed prerequisite, it waits
	// until wgPrerequisites counts down to zero then starts execution.
	started atomic.Bool
}

func getCtxWithTimeout(timeout time.Duration) context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	return ctx
}

// Run this stage and trigger its downstream stages.
func (s *Stage) Run(ctx context.Context) []*QueryResult {
	if s.States.RunName == "" {
		s.States.RunName = s.Id
	}
	// If output path was not specified, use the current directory as a fallback.
	if s.States.OutputPath == "" {
		s.States.OutputPath = s.BaseDir
	}
	s.States.OutputPath = filepath.Join(s.States.OutputPath, s.States.RunName)
	if err := os.MkdirAll(s.States.OutputPath, 0755); err != nil {
		log.Fatal().Str("output_path", s.States.OutputPath).Err(err).Msg("failed to create output directory")
	} else {
		log.Info().Str("output_path", s.States.OutputPath).Msg("output will be saved to this path")
	}

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(s.States.OutputPath, s.States.RunName+".log")
	if logFile, err := os.OpenFile(logPath, OpenNewFileFlags, 0644); err != nil {
		log.Error().Str("log_path", logPath).Err(err).Msg("failed to create the log file")
		// In this case, the global logger is not changed. Log messages are still printed to stderr.
	} else {
		bufWriter := bufio.NewWriter(logFile)
		defer func() {
			_ = bufWriter.Flush()
			_ = logFile.Close()
		}()
		log.SetGlobalLogger(zerolog.New(io.MultiWriter(os.Stderr, bufWriter)).With().Timestamp().Stack().Logger())
		log.Info().Str("log_path", logPath).Msg("log file will be saved to this path")
	}

	// This initial size is just a good start, might not be enough.
	results := make([]*QueryResult, 0, len(s.Queries)+len(s.QueryFiles))
	s.States.resultChan = make(chan *QueryResult, 16)
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, os.Interrupt, os.Kill)
	// Each goroutine we spawn will increment this wait group (count-down latch). We may start a goroutine for running
	// a benchmark stage, or write query output to disk asynchronously.
	// This wait group is propagated to the descendant stages.
	s.States.wgExitMainStage = &sync.WaitGroup{}
	// Increment the wait group by 1 immediately for this main stage itself, which will also be a goroutine.
	s.States.wgExitMainStage.Add(1)

	go func() {
		s.States.wgExitMainStage.Wait()
		// wgExitMainStage goes down to 0 after all the goroutines finish. Then we exit the driver by
		// closing the timeToExit channel, which will trigger the graceful shutdown process -
		// (flushing the log file, writing the final time log summary, etc.).

		// When SIGKILL and SIGINT are captured, we trigger this process by canceling the context, which will cause
		// "context cancelled" errors in goroutines to let them exit.
		close(timeToExit)
	}()

	ctx, s.States.AbortAll = context.WithCancelCause(ctx)
	log.Debug().EmbedObject(s).Msg("created cancellable context")
	// Start to run the queries defined in this main stage in a goroutine, which is managed exactly like all other
	// concurrent stages.
	go func() {
		_ = s.run(ctx)
	}()

	for {
		select {
		case result := <-s.States.resultChan:
			results = append(results, result)
			for _, recorder := range s.States.runRecorders {
				recorder.RecordQuery(getCtxWithTimeout(time.Second*5), s, result)
			}
		case sig := <-timeToExit:
			if sig != nil {
				// Cancel the context and wait for the goroutines to exit.
				s.States.AbortAll(fmt.Errorf(sig.String()))
				continue
			}
			s.States.RunFinishTime = time.Now()
			for _, recorder := range s.States.runRecorders {
				recorder.RecordRun(getCtxWithTimeout(time.Second*5), s, results)
			}
			return results
		}
	}
}

// Note, we do not handle errors outside of this run method. the returnErr was there for the deferred function
// to take actions based on its value.
func (s *Stage) run(ctx context.Context) (returnErr error) {
	if !s.started.CompareAndSwap(false, true) {
		// If other prerequisites finished earlier, then this stage is already called and waiting.
		s.States.wgExitMainStage.Done()
		return nil
	}
	defer func() {
		// Remember to unblock the descendant stages no matter this stage threw an error or not.
		for _, nextStage := range s.NextStages {
			nextStage.wgPrerequisites.Done()
		}
		if returnErr != nil {
			s.logErr(ctx, returnErr)
			if *s.AbortOnError {
				log.Debug().EmbedObject(s).Msg("canceling the context because abort_on_error is set to true")
				s.States.AbortAll(returnErr)
			}
		} else {
			// Trigger descendant stages.
			s.States.wgExitMainStage.Add(len(s.NextStages))
			for _, nextStage := range s.NextStages {
				go func(nextStage *Stage) {
					_ = nextStage.run(ctx)
				}(nextStage)
			}
		}
		s.States.wgExitMainStage.Done()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.waitForPrerequisites():
	}
	log.Debug().EmbedObject(s).Msg("all prerequisites finished")
	s.setDefaults()
	s.prepareClient()
	s.propagateStates()
	if s.RandomExecution {
		return s.runRandomly(ctx)
	}
	return s.runSequentially(ctx)
}

func (s *Stage) runSequentially(ctx context.Context) (returnErr error) {
	// Try to match an array of expected row counts
	keyToMatch := s.currentCatalog + "." + s.currentSchema
	if erc, exists := s.ExpectedRowCounts[keyToMatch]; exists {
		s.expectedRowCountInCurrentSchema = erc
	} else if erc, exists = s.ExpectedRowCounts[s.currentSchema]; exists {
		s.expectedRowCountInCurrentSchema = erc
	} else {
		for k, v := range s.ExpectedRowCounts {
			if matcher, err := regexp.Compile(k); err != nil {
				continue
			} else if matcher.MatchString(keyToMatch) {
				s.expectedRowCountInCurrentSchema = v
				break
			}
		}
	}
	if err := s.runQueries(ctx, s.Queries, nil, 0); err != nil {
		return err
	}
	expectedRowCountStartIndex := len(s.Queries)
	// Using range loop variable may cause some problems here because the file names can be propagated to some other
	// goroutines that may use this query file path string.
	for i := 0; i < len(s.QueryFiles); i++ {
		if err := s.runQueryFile(ctx, s.QueryFiles[i], &expectedRowCountStartIndex, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stage) runQueryFile(ctx context.Context, queryFile string, expectedRowCountStartIndex *int, fileAlias *string) (returnErr error) {
	queryFileAbsPath := queryFile
	if !filepath.IsAbs(queryFileAbsPath) {
		queryFileAbsPath = filepath.Join(s.BaseDir, queryFileAbsPath)
	}
	file, err := os.Open(queryFileAbsPath)
	var queries []string
	if err == nil {
		queries, err = presto.SplitQueries(file)
	}
	if err != nil {
		if !*s.AbortOnError {
			log.Error().Err(err).Str("file_path", queryFile).Msg("failed to read queries from file")
			err = nil
		}
		// If we run into errors reading the query file, then the offset in the expected row count array will be messed up.
		// Reset it to nil to stop showing expected row counts and to avoid confusions.
		s.expectedRowCountInCurrentSchema = nil
		return err
	}
	if fileAlias == nil {
		fileAlias = &queryFile
	}
	if expectedRowCountStartIndex != nil {
		err = s.runQueries(ctx, queries, fileAlias, *expectedRowCountStartIndex)
		*expectedRowCountStartIndex += len(queries)
	} else {
		err = s.runQueries(ctx, queries, fileAlias, 0)
	}
	return err
}

func (s *Stage) runRandomly(ctx context.Context) (returnErr error) {
	var continueExecution func(queryCount int) bool
	if dur, parseErr := time.ParseDuration(s.RandomlyExecuteUntil); parseErr == nil {
		endTime := time.Now().Add(dur)
		continueExecution = func(_ int) bool {
			return time.Now().Before(endTime)
		}
	} else if count, atoiErr := strconv.Atoi(s.RandomlyExecuteUntil); atoiErr == nil {
		continueExecution = func(queryCount int) bool {
			return queryCount <= count
		}
	} else {
		err := fmt.Errorf("failed to parse randomly_execute_until %s", s.RandomlyExecuteUntil)
		if *s.AbortOnError {
			return err
		} else {
			log.Error().Err(err).Send()
			return nil
		}
	}
	r := rand.New(rand.NewSource(s.States.RandSeed))
	s.States.RandSeedUsed = true
	log.Info().Int64("seed", s.States.RandSeed).Msg("random source seeded")
	randIndexUpperBound := len(s.Queries) + len(s.QueryFiles)
	for i := 1; continueExecution(i); i++ {
		idx := r.Intn(randIndexUpperBound)
		if i <= s.States.RandSkip {
			if i == s.States.RandSkip {
				log.Info().Msgf("skipped %d random selections", i)
			}
			continue
		}
		if idx < len(s.Queries) {
			// Run query embedded in the json file.
			pseudoFileName := fmt.Sprintf("rand_%d", i)
			if err := s.runQueries(ctx, s.Queries[idx:idx+1], &pseudoFileName, 0); err != nil {
				return err
			}
		} else {
			queryFile := s.QueryFiles[idx-len(s.Queries)]
			fileAlias := fmt.Sprintf("rand_%d/%s", i, queryFile)
			if err := s.runQueryFile(ctx, queryFile, nil, &fileAlias); err != nil {
				return err
			}
		}
	}
	log.Info().Msg("random execution concluded.")
	return nil
}

func (s *Stage) runQueries(ctx context.Context, queries []string, queryFile *string, expectedRowCountStartIndex int) (retErr error) {
	batchSize := len(queries)
	for i, queryText := range queries {
		for j := 0; j < s.ColdRuns+s.WarmRuns; j++ {
			query := &Query{
				Text:             queryText,
				File:             queryFile,
				Index:            i,
				BatchSize:        batchSize,
				ColdRun:          j < s.ColdRuns,
				SequenceNo:       j,
				ExpectedRowCount: -1, // -1 means unspecified.
			}
			if len(s.expectedRowCountInCurrentSchema) > expectedRowCountStartIndex+i {
				query.ExpectedRowCount = s.expectedRowCountInCurrentSchema[expectedRowCountStartIndex+i]
			}

			result, err := s.runQuery(ctx, query)
			// err is already attached to the result, if not nil.
			if s.States.OnQueryCompletion != nil {
				s.States.OnQueryCompletion(result)
			}
			// Flags and options are checked within.
			s.saveQueryJsonFile(result)
			// Each query should have a query result sent to the channel, no matter
			// its execution succeeded or not.
			s.States.resultChan <- result
			if err != nil {
				if *s.AbortOnError || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					// If AbortOnError is set, we skip the rest queries in the same batch.
					// Logging etc. will be handled in the parent stack.
					// If the context is cancelled or timed out, we cannot continue whatsoever and must return.
					return result
				}
				// Log the error information and continue running
				s.logErr(ctx, result)
				continue
			}
			log.Info().EmbedObject(result).Msgf("query finished")
		}
	}
	return nil
}

func (s *Stage) runQuery(ctx context.Context, query *Query) (result *QueryResult, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().EmbedObject(s).Msgf("recovered from panic: %v", r)
			if e, ok := r.(error); ok {
				retErr = e
			}
		}
		result.QueryError = retErr
		result.ConcludeExecution()
	}()

	result = &QueryResult{
		StageId:   s.Id,
		Query:     query,
		StartTime: time.Now(),
	}

	select {
	case <-ctx.Done():
		return result, ctx.Err()
	default:
	}

	querySourceStr := s.querySourceString(result)
	clientResult, _, err := s.Client.Query(ctx, query.Text,
		func(req *http.Request) {
			req.Header.Set(presto.SourceHeader, querySourceStr)
		})
	if clientResult != nil {
		result.QueryId = clientResult.Id
		result.InfoUrl = clientResult.InfoUri
	}
	if err != nil {
		return result, err
	}

	// Log query submission
	e := log.Debug().EmbedObject(result.SimpleLogging())
	if s.currentCatalog != "" {
		e = e.Str("catalog", s.currentCatalog)
	}
	if s.currentSchema != "" {
		e = e.Str("schema", s.currentSchema)
	}
	if s.currentTimeZone != "" {
		e = e.Str("timezone", s.currentTimeZone)
	}
	if *s.SaveOutput {
		e.Bool("save_output", true)
	}
	if *s.SaveColumnMetadata {
		e.Bool("save_column_metadata", true)
	}
	e.Msgf("submitted query")

	var (
		queryOutputFile   *os.File
		queryOutputWriter *bufio.Writer
		queryOutputChan   chan []json.RawMessage
	)
	if *s.SaveOutput {
		queryOutputFile, err = os.OpenFile(
			filepath.Join(s.States.OutputPath, querySourceStr)+".output",
			OpenNewFileFlags, 0644)
		if err != nil {
			return result, err
		}
		queryOutputWriter = bufio.NewWriterSize(queryOutputFile, 8192)
		queryOutputChan = make(chan []json.RawMessage)
		defer close(queryOutputChan)

		// Start a goroutine to write query output in the background.
		// Make sure the main stage won't exit until this background goroutine finishes.
		s.States.wgExitMainStage.Add(1)
		go func() {
			for data := range queryOutputChan {
				for _, row := range data {
					_, ioErr := queryOutputWriter.Write(row)
					if ioErr == nil {
						ioErr = queryOutputWriter.WriteByte('\n')
					}
					if ioErr != nil {
						log.Error().Err(ioErr).EmbedObject(result.SimpleLogging()).
							Msg("failed to write query result")
						// Skip the current batch on error.
						break
					}
				}
			}
			if ioErr := queryOutputWriter.Flush(); ioErr != nil {
				log.Error().Err(ioErr).EmbedObject(result.SimpleLogging()).
					Msg("failed to write query result")
			} else {
				log.Debug().EmbedObject(result.SimpleLogging()).Msg("query data saved successfully")
			}
			_ = queryOutputFile.Close()
			s.States.wgExitMainStage.Done()
		}()
	}

	err = clientResult.Drain(ctx, func(qr *presto.QueryResults) error {
		result.RowCount += len(qr.Data)
		if queryOutputWriter != nil {
			queryOutputChan <- qr.Data
		}
		if qr.NextUri == nil {
			_ = s.saveColumnMetadataFile(qr, result, querySourceStr)
		}
		return nil
	})

	return result, err
}
