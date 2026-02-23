package stage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/prestoapi"
	"pbench/utils"

	"reflect"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	presto "github.com/ethanyzhang/presto-go"

	"github.com/rs/zerolog"
)

type Stage struct {
	// Id is used to uniquely identify a stage. It is usually the file name without its directory path and extension.
	Id string `json:"id,omitempty"`
	// Description is an optional human-readable description of what this stage does.
	Description *string `json:"description,omitempty"`
	// The values in Catalog, Schema, and SessionParams are inherited by the descendant stages. Catalog, Schema,
	// and TimeZone are auto-detected: if a child stage sets a different value than what the inherited session has,
	// a new client session is automatically created. You can also force a new client session by setting StartOnNewClient = true.
	Catalog       *string        `json:"catalog,omitempty"`
	Schema        *string        `json:"schema,omitempty"`
	SessionParams map[string]any `json:"session_params,omitempty"`
	TimeZone      *string        `json:"timezone,omitempty"`
	Queries       []string       `json:"queries,omitempty"`
	// If a stage has both Queries and QueryFiles, the queries in the Queries array will be executed first then
	// the QueryFiles will be read and executed.
	QueryFiles []string `json:"query_files,omitempty"`
	// Run shell scripts before starting the execution of queries in a stage.
	PreStageShellScripts []string `json:"pre_stage_scripts,omitempty"`
	// Run shell scripts after executing all the queries in a stage.
	PostStageShellScripts []string `json:"post_stage_scripts,omitempty"`
	// Run shell scripts before executing each query.
	PreQueryShellScripts []string `json:"pre_query_scripts,omitempty"`
	// Run shell scripts after executing each query.
	PostQueryShellScripts []string `json:"post_query_scripts,omitempty"`
	// Run shell scripts before starting all runs (cold + warm) of each individual query.
	PreQueryCycleShellScripts []string `json:"pre_query_cycle_scripts,omitempty"`
	// Run shell scripts after all runs (cold + warm) of each individual query have completed.
	PostQueryCycleShellScripts []string `json:"post_query_cycle_scripts,omitempty"`
	// A map from [catalog.schema] to arrays of integers as expected row counts for all the queries we run
	// under different schemas. This includes the queries from both Queries and QueryFiles. Queries first and QueryFiles follows.
	// Can use regexp as key to match multiple [catalog.schema] pairs.
	ExpectedRowCounts map[string][]int `json:"expected_row_counts,omitempty"`
	// When RandomExecution is turned on, we randomly pick queries to run until a certain number of queries/a specific
	// duration has passed. Expected row counts will not be checked in this mode because we cannot figure out the correct
	// expected row count offset (query files are treated as black boxes).
	RandomExecution *bool `json:"random_execution,omitempty"`
	// Use RandomlyExecuteUntil to specify a duration like "1h" or an integer as the number of queries should be executed
	// before exiting.
	RandomlyExecuteUntil *string `json:"randomly_execute_until,omitempty"`
	// If NoRandomDuplicates is true, queries are shuffled and each is executed once before any repeats.
	// Only effective when RandomExecution is true.
	NoRandomDuplicates *bool `json:"no_random_duplicates,omitempty"`
	// If not set, the default is 1. The default value is set when the stage is run.
	ColdRuns *int `json:"cold_runs,omitempty" validate:"omitempty,gte=0"`
	// If not set, the default is 0.
	WarmRuns *int `json:"warm_runs,omitempty" validate:"omitempty,gte=0"`
	// If StartOnNewClient is set to true, this stage will create a new client session to execute itself.
	// This new client session will be passed down to its children stages unless those stages also set StartOnNewClient to true.
	// Each client session can carry its own set of client information, tags, session properties, etc.
	// Children stages will **NOT** inherit this value from their parents so this is declared as a value not a pointer.
	StartOnNewClient bool `json:"start_on_new_client,omitempty"`
	// If AbortOnError is set to true, the context associated with this stage will be canceled if an error occurs.
	// Depending on when the cancellable context was created, this may abort some or all other running stages and all future stages.
	// Children stages will inherit this value from their parent if it is not set (nil).
	AbortOnError *bool `json:"abort_on_error,omitempty"`
	// If SaveOutput is set to true, the query result will be saved to files in its raw form.
	// Children stages will inherit this value from their parent if it is not set.
	SaveOutput *bool `json:"save_output,omitempty"`
	// If SaveColumnMetadata is set to true, we will save a json file of the query result's column metadata.
	// See the "columns" field in Presto's query API response.
	// Children stages will inherit this value from their parent if it is not set.
	SaveColumnMetadata *bool `json:"save_column_metadata,omitempty"`
	// If SaveJson is set to true, the query json will be saved to files in its raw form after the query is executed.
	// Children stages will inherit this value from their parent if it is not set.
	// When a query failed to execute for whatever reason, a query json file will be automatically saved even if this
	// knob was not set to true.
	SaveJson       *bool    `json:"save_json,omitempty"`
	NextStagePaths []string `json:"next,omitempty"`
	// StreamCount specifies how many parallel instances of this stage should run.
	// Each stream gets a deterministically derived seed for reproducible randomization.
	// Not inherited by child stages.
	StreamCount *int `json:"stream_count,omitempty" validate:"omitempty,gte=1"`

	// BaseDir is set to the directory path of this stage's location. It is used to locate the children stages when
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
	// propagateOnce ensures that when a child stage has multiple parents, only the first parent to finish
	// propagates state into the child. This prevents a data race where multiple parents write concurrently.
	propagateOnce sync.Once
}

// Run this stage and trigger its downstream stages.
func (s *Stage) Run(ctx context.Context) int {
	if s.States == nil {
		s.InitStates()
	}
	if s.States.RunName == "" {
		s.States.RunName = s.Id
	}
	// If output path was not specified, use the current directory as a fallback.
	if s.States.OutputPath == "" {
		s.States.OutputPath = s.BaseDir
	}
	s.States.OutputPath = filepath.Join(s.States.OutputPath, s.States.RunName)
	utils.PrepareOutputDirectory(s.States.OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(s.States.OutputPath, s.States.RunName+".log")
	flushLog := utils.InitLogFile(logPath)
	defer flushLog()

	// This initial size is just a good start, might not be enough.
	results := make([]*QueryResult, 0, len(s.Queries)+len(s.QueryFiles))
	s.States.resultChan = make(chan *QueryResult, 16)
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
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

		// Deregister signal delivery before closing the channel to prevent a panic
		// ("send on closed channel") if a signal arrives after close.
		signal.Stop(timeToExit)
		close(timeToExit)
	}()

	ctx, s.States.AbortAll = context.WithCancelCause(ctx)
	log.Debug().EmbedObject(s).Msg("created cancellable context")
	for _, recorder := range s.States.runRecorders {
		if err := recorder.Start(ctx, s); err != nil {
			log.Fatal().Err(err).Msgf("failed to prepare %s", reflect.TypeOf(recorder).Name())
		}
	}
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
				rCtx, rCancel := utils.GetCtxWithTimeout(time.Second * 5)
				recorder.RecordQuery(rCtx, s, result)
				rCancel()
			}
		case sig := <-timeToExit:
			if sig != nil {
				// Cancel the context and wait for the goroutines to exit.
				s.States.AbortAll(fmt.Errorf("%s", sig.String()))
				continue
			}
			// All stage goroutines are done (WaitGroup hit 0). Drain any results
			// still buffered in resultChan — the select may have picked timeToExit
			// over resultChan when both were ready simultaneously.
		drainLoop:
			for {
				select {
				case result := <-s.States.resultChan:
					results = append(results, result)
					for _, recorder := range s.States.runRecorders {
						rCtx, rCancel := utils.GetCtxWithTimeout(time.Second * 5)
						recorder.RecordQuery(rCtx, s, result)
						rCancel()
					}
				default:
					break drainLoop
				}
			}
			// Derive RunFinishTime from the latest query EndTime, which is set by
			// ConcludeExecution() right after query data is drained — before I/O
			// teardown (saveQueryJsonFile, output file flushes). This gives a clean
			// "last query completion" time that excludes post-run I/O.
			for _, result := range results {
				if result.EndTime != nil && result.EndTime.After(s.States.RunFinishTime) {
					s.States.RunFinishTime = *result.EndTime
				}
			}
			for _, recorder := range s.States.runRecorders {
				rCtx, rCancel := utils.GetCtxWithTimeout(time.Second * 5)
				recorder.RecordRun(rCtx, s, results)
				rCancel()
			}
			return int(s.States.exitCode.Load())
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
		if returnErr != nil {
			s.logErr(ctx, returnErr)
			if *s.AbortOnError {
				log.Debug().EmbedObject(s).Msg("canceling the context because abort_on_error is set to true")
				// Cancel context BEFORE unblocking children so they see the cancellation immediately.
				s.States.AbortAll(returnErr)
			}
		}
		// Unblock children stages no matter this stage threw an error or not.
		for _, nextStage := range s.NextStages {
			nextStage.wgPrerequisites.Done()
		}
		if returnErr == nil {
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
	log.Info().EmbedObject(s).Msg("all prerequisites finished")
	s.setDefaults()
	s.prepareClient()
	s.propagateStates()
	preStageErr := s.runShellScripts(ctx, s.PreStageShellScripts)
	if preStageErr != nil {
		return fmt.Errorf("pre-stage script execution failed: %w", preStageErr)
	}
	if len(s.Queries)+len(s.QueryFiles) > 0 {
		if s.StreamCount != nil && *s.StreamCount > 1 {
			returnErr = s.runAsMultipleStreams(ctx)
		} else if *s.RandomExecution {
			returnErr = s.runRandomly(ctx, s.States.RandSeed)
		} else {
			returnErr = s.runSequentially(ctx)
		}
	} else {
		log.Info().Msg("no query to run.")
	}

	postStageErr := s.runShellScripts(ctx, s.PostStageShellScripts)
	returnErr = errors.Join(returnErr, postStageErr)
	return
}

// runAsMultipleStreams executes this stage as multiple parallel stream instances.
// Each stream is a new Stage that copies the relevant fields from this stage.
func (s *Stage) runAsMultipleStreams(ctx context.Context) error {
	streamCount := *s.StreamCount
	log.Info().EmbedObject(s).Int("stream_count", streamCount).Msg("starting parallel stream execution")

	errChan := make(chan error, streamCount)
	var wg sync.WaitGroup
	for i := range streamCount {
		wg.Add(1)
		stream := s.newStreamInstance(i)
		seed := s.States.RandSeed + int64(i)*1000

		go func() {
			defer wg.Done()
			var err error
			if *stream.RandomExecution {
				err = stream.runRandomly(ctx, seed)
			} else {
				err = stream.runSequentially(ctx)
			}
			if err != nil {
				log.Error().Err(err).Str("stream", stream.Id).Msg("stream failed")
				errChan <- err
			} else {
				log.Info().Str("stream", stream.Id).Msg("stream completed")
			}
		}()
	}
	wg.Wait()
	close(errChan)

	var errs []error
	for err := range errChan {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// newStreamInstance creates a new Stage for a stream, copying the fields needed for query execution.
// Sync primitives (wgPrerequisites, started, propagateOnce) are left at zero values since
// streams don't participate in DAG coordination.
func (s *Stage) newStreamInstance(index int) *Stage {
	return &Stage{
		Id:                         fmt.Sprintf("%s_stream_%d", s.Id, index+1),
		Catalog:                    s.Catalog,
		Schema:                     s.Schema,
		SessionParams:              s.SessionParams,
		TimeZone:                   s.TimeZone,
		Queries:                    s.Queries,
		QueryFiles:                 s.QueryFiles,
		ExpectedRowCounts:          s.ExpectedRowCounts,
		RandomExecution:            s.RandomExecution,
		RandomlyExecuteUntil:       s.RandomlyExecuteUntil,
		NoRandomDuplicates:         s.NoRandomDuplicates,
		ColdRuns:                   s.ColdRuns,
		WarmRuns:                   s.WarmRuns,
		AbortOnError:               s.AbortOnError,
		SaveOutput:                 s.SaveOutput,
		SaveColumnMetadata:         s.SaveColumnMetadata,
		SaveJson:                   s.SaveJson,
		PreQueryShellScripts:       s.PreQueryShellScripts,
		PostQueryShellScripts:      s.PostQueryShellScripts,
		PreQueryCycleShellScripts:  s.PreQueryCycleShellScripts,
		PostQueryCycleShellScripts: s.PostQueryCycleShellScripts,
		BaseDir:                    s.BaseDir,
		States:                     s.States,
		Client:                     s.Client,
		currentCatalog:             s.currentCatalog,
		currentSchema:              s.currentSchema,
		currentTimeZone:            s.currentTimeZone,
	}
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

func (s *Stage) runQueryFile(ctx context.Context, queryFile string, expectedRowCountStartIndex *int, fileAlias *string) error {
	// fileAlias is the query file name we will report. We try to make it short, so try to use relative path when possible.
	if fileAlias == nil {
		if relPath, relErr := filepath.Rel(s.BaseDir, queryFile); relErr == nil {
			fileAlias = &relPath
		} else {
			fileAlias = &queryFile
		}
	}

	file, err := os.Open(queryFile)
	var queries []string
	if err == nil {
		queries, err = prestoapi.SplitQueries(file)
		file.Close()
	}
	if err != nil {
		if !*s.AbortOnError {
			log.Error().Err(err).Str("file_path", queryFile).Msg("failed to read queries from file")
			err = nil
			// If we run into errors reading the query file, then the offset in the expected row count array will be messed up.
			// Reset it to nil to stop showing expected row counts and to avoid confusions.
			s.expectedRowCountInCurrentSchema = nil
		} else {
			s.States.exitCode.CompareAndSwap(0, 1)
		}
		return err
	}

	if expectedRowCountStartIndex != nil {
		err = s.runQueries(ctx, queries, fileAlias, *expectedRowCountStartIndex)
		*expectedRowCountStartIndex += len(queries)
	} else {
		err = s.runQueries(ctx, queries, fileAlias, 0)
	}
	return err
}

func (s *Stage) runRandomly(ctx context.Context, seed int64) error {
	if s.RandomlyExecuteUntil == nil {
		return fmt.Errorf("random_execution is true but randomly_execute_until is not set")
	}
	var continueExecution func(queryCount int) bool
	if dur, parseErr := time.ParseDuration(*s.RandomlyExecuteUntil); parseErr == nil {
		endTime := time.Now().Add(dur)
		continueExecution = func(_ int) bool {
			return time.Now().Before(endTime)
		}
	} else if count, atoiErr := strconv.Atoi(*s.RandomlyExecuteUntil); atoiErr == nil {
		continueExecution = func(queryCount int) bool {
			return queryCount <= count
		}
	} else {
		err := fmt.Errorf("failed to parse randomly_execute_until %s", *s.RandomlyExecuteUntil)
		if *s.AbortOnError {
			s.States.exitCode.CompareAndSwap(0, 2) // syntax error
			return err
		} else {
			log.Error().Err(err).Send()
			return nil
		}
	}
	r := rand.New(rand.NewSource(seed))
	s.States.RandSeedUsed.Store(true)
	log.Info().EmbedObject(s).Int64("seed", seed).Msg("random source seeded")
	totalQueries := len(s.Queries) + len(s.QueryFiles)

	// nextIndex returns the next query index to execute.
	// When no_random_duplicates is true, it shuffles all indices and cycles through them,
	// ensuring each query runs once before any repeats.
	var indices []int
	var pos int
	nextIndex := func() int {
		if *s.NoRandomDuplicates {
			if indices == nil || pos >= len(indices) {
				indices = r.Perm(totalQueries)
				pos = 0
			}
			idx := indices[pos]
			pos++
			return idx
		}
		return r.Intn(totalQueries)
	}

	for i := 1; continueExecution(i); i++ {
		idx := nextIndex()
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
			fileAlias := queryFile
			if relPath, relErr := filepath.Rel(s.BaseDir, queryFile); relErr == nil {
				fileAlias = relPath
			}
			fileAlias = fmt.Sprintf("rand_%d_%s", i, fileAlias)
			if err := s.runQueryFile(ctx, queryFile, nil, &fileAlias); err != nil {
				return err
			}
		}
	}
	log.Info().Msg("random execution concluded.")
	return nil
}

func (s *Stage) runShellScripts(ctx context.Context, shellScripts []string, extraEnv ...string) error {
	for i, script := range shellScripts {
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", script)
		cmd.Dir = s.BaseDir
		cmd.Env = append(os.Environ(),
			"PBENCH_STAGE_ID="+s.Id,
			"PBENCH_OUTPUT_DIR="+s.States.OutputPath,
		)
		cmd.Env = append(cmd.Env, extraEnv...)
		outBuf, errBuf := new(bytes.Buffer), new(bytes.Buffer)
		cmd.Stdout, cmd.Stderr = outBuf, errBuf
		var logEntry *zerolog.Event
		err := cmd.Run()
		if err != nil {
			logEntry = log.Error()
		} else {
			logEntry = log.Info()
		}
		logEntry.EmbedObject(s).Int("script_index", i).Str("script", script).
			Int("exit_code", cmd.ProcessState.ExitCode()).Str("status", cmd.ProcessState.String()).
			Dur("system_time", cmd.ProcessState.SystemTime()).Str("stdout", outBuf.String()).
			Str("stderr", errBuf.String())
		if err != nil {
			logEntry.Err(err).Msg("run shell script failed.")
			if *s.AbortOnError || ctx.Err() != nil {
				s.States.exitCode.CompareAndSwap(0, int32(cmd.ProcessState.ExitCode()))
				return err
			}
		} else {
			logEntry.Msg("run shell script")
		}
	}
	return nil
}

func (s *Stage) runQueries(ctx context.Context, queries []string, queryFile *string, expectedRowCountStartIndex int) (retErr error) {
	batchSize := len(queries)
	for i, queryText := range queries {
		queryCycleEnv := s.queryCycleEnv(queryFile, i)
		// run pre query cycle shell scripts
		preQueryCycleErr := s.runShellScripts(ctx, s.PreQueryCycleShellScripts, queryCycleEnv...)
		if preQueryCycleErr != nil {
			return fmt.Errorf("pre-query script execution failed: %w", preQueryCycleErr)
		}
		var abortErr error
		for j := 0; j < *s.ColdRuns+*s.WarmRuns; j++ {
			query := &Query{
				Text:             queryText,
				File:             queryFile,
				Index:            i,
				BatchSize:        batchSize,
				ColdRun:          j < *s.ColdRuns,
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
				if *s.AbortOnError || ctx.Err() != nil {
					// Break instead of returning so post_query_cycle_scripts still runs as
					// the teardown counterpart to pre_query_cycle_scripts.
					s.States.exitCode.CompareAndSwap(0, 1)
					abortErr = result
					break
				}
				// Log the error information and continue running
				s.logErr(ctx, result)
				continue
			}
			log.Info().EmbedObject(result).Msgf("query finished")
		}
		// run post query cycle shell scripts
		postQueryCycleErr := s.runShellScripts(ctx, s.PostQueryCycleShellScripts, queryCycleEnv...)
		if abortErr != nil {
			return abortErr
		}
		if postQueryCycleErr != nil {
			return fmt.Errorf("post-query script execution failed: %w", postQueryCycleErr)
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
			} else {
				retErr = fmt.Errorf("panic: %v", r)
			}
		}
		if result == nil {
			result = &QueryResult{StageId: s.Id, Query: query, StartTime: time.Now()}
		}
		result.QueryError = retErr
		result.ConcludeExecution()
	}()

	result = &QueryResult{
		StageId:   s.Id,
		Query:     query,
		StartTime: time.Now(),
	}

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// run pre query shell scripts
	preQueryErr := s.runShellScripts(ctx, s.PreQueryShellScripts, s.queryEnv(query, nil, nil)...)
	if preQueryErr != nil {
		return result, preQueryErr
	}

	querySourceStr := s.querySourceString(result)
	clientResult, _, err := s.Client.Query(ctx, query.Text,
		func(req *http.Request) {
			req.Header.Set(s.Client.CanonicalHeader(presto.SourceHeader), querySourceStr)
		})
	if clientResult != nil {
		result.QueryId = clientResult.Id
		result.InfoUrl = clientResult.InfoUri
	}
	if err != nil {
		return result, err
	}

	// Log query submission
	e := log.Info().EmbedObject(result.SimpleLogging())
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
			utils.OpenNewFileFlags, 0644)
		if err != nil {
			return result, err
		}
		queryOutputWriter = bufio.NewWriterSize(queryOutputFile, 8192)
		queryOutputChan = make(chan []json.RawMessage)
		defer close(queryOutputChan)

		// Start a goroutine to write query output in the background.
		// Make sure the main stage won't exit until this background goroutine finishes.
		// Capture a snapshot for logging to avoid racing with the defer that writes result.QueryError.
		resultSnapshot := result.SimpleLogging()
		s.States.wgExitMainStage.Add(1)
		go func() {
			for data := range queryOutputChan {
				for _, row := range data {
					_, ioErr := queryOutputWriter.Write(row)
					if ioErr == nil {
						ioErr = queryOutputWriter.WriteByte('\n')
					}
					if ioErr != nil {
						log.Error().Err(ioErr).EmbedObject(resultSnapshot).
							Msg("failed to write query result")
						// Skip the current batch on error.
						break
					}
				}
			}
			if ioErr := queryOutputWriter.Flush(); ioErr != nil {
				log.Error().Err(ioErr).EmbedObject(resultSnapshot).
					Msg("failed to write query result")
			} else {
				log.Info().EmbedObject(resultSnapshot).Msg("query data saved successfully")
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
		if qr.NextUri == nil && query.SequenceNo == 0 {
			_ = s.saveColumnMetadataFile(qr, result, querySourceStr)
		}
		return nil
	})
	// run post query shell scripts
	postQueryErr := s.runShellScripts(ctx, s.PostQueryShellScripts, s.queryEnv(query, result, err)...)
	err = errors.Join(err, postQueryErr)
	return result, err
}
