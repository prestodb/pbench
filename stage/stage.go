package stage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	Queries       []string       `json:"queries,omitempty"`
	// If a stage has both Queries and QueryFiles, the queries in the Queries array will be executed first then
	// the QueryFiles will be read and executed.
	QueryFiles []string `json:"query_files,omitempty"`
	// If not set, the default is 0.
	ColdRuns int `json:"cold_runs,omitempty"`
	// If not set, the default is 1. The default value is set when the stage is run.
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

	// wgPrerequisites is a count-down latch to wait for all the prerequisites to finish before starting this stage.
	wgPrerequisites sync.WaitGroup

	// When a stage has multiple prerequisites, it will be called when each prerequisite stage finishes. started is here
	// to make sure this stage only starts once. When the stage is started by its first completed prerequisite, it waits
	// until wgPrerequisites counts down to zero then starts execution.
	started atomic.Bool
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

	summaryBuilder := &strings.Builder{}
	summaryBuilder.WriteString("stage_id,query_file,query_index,cold_run,run_index,info_url,succeeded,row_count,start_time,end_time,duration_in_seconds\n")
	for {
		select {
		case result := <-s.States.resultChan:
			results = append(results, result)
			s.appendQuerySummary(summaryBuilder, result)
			s.sendQuerySummaryToInfluxDB(ctx, result)
		case sig := <-timeToExit:
			if sig != nil {
				// Cancel the context and wait for the goroutines to exit.
				s.States.AbortAll(fmt.Errorf(sig.String()))
				continue
			}
			s.States.RunFinishTime = time.Now()
			_ = os.WriteFile(filepath.Join(s.States.OutputPath, s.Id+"_summary.csv"), []byte(summaryBuilder.String()), 0644)
			s.sendRunSummaryToInfluxDB(context.Background(), results)
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
	if err := s.runQueries(ctx, s.Queries, nil); err != nil {
		return err
	}
	// Using range loop variable may cause some problems here because the file names can be propagated to some other
	// goroutines that may use this query file path string.
	for i := 0; i < len(s.QueryFiles); i++ {
		queryFileAbsPath := s.QueryFiles[i]
		if !filepath.IsAbs(queryFileAbsPath) {
			queryFileAbsPath = filepath.Join(s.BaseDir, queryFileAbsPath)
		}
		file, err := os.Open(queryFileAbsPath)
		if err != nil {
			return err
		}
		queries, err := presto.SplitQueries(file)
		if err != nil {
			return err
		}
		err = s.runQueries(ctx, queries, &s.QueryFiles[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Stage) runQueries(ctx context.Context, queries []string, queryFile *string) (retErr error) {
	batchSize := len(queries)
	for i, queryText := range queries {
		for j := 0; j < s.ColdRuns+s.WarmRuns; j++ {
			query := &Query{
				Text:      queryText,
				File:      queryFile,
				Index:     i,
				BatchSize: batchSize,
				ColdRun:   j < s.ColdRuns,
				RunIndex:  j,
			}

			result, err := s.runQuery(ctx, query)
			// err is already attached to the result, if not nil.
			if s.States.OnQueryCompletion != nil {
				s.States.OnQueryCompletion(result)
			}
			// Flags and options are checked within.
			s.saveQueryJsonFile(ctx, result)
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
	if catalog := s.Client.GetCatalog(); catalog != "" {
		e = e.Str("catalog", catalog)
	}
	if schema := s.Client.GetSchema(); schema != "" {
		e = e.Str("schema", schema)
	}
	if *s.SaveOutput {
		e.Bool("save_output", true)
	}
	if *s.SaveColumnMetadata {
		e.Bool("save_column_metadata", true)
	}
	e.Msgf("submitted query")

	var (
		queryOutputFile *os.File
		queryOutput     *bufio.Writer
		wgQueryOutput   *sync.WaitGroup
	)
	if *s.SaveOutput {
		queryOutputFile, err = os.OpenFile(
			filepath.Join(s.States.OutputPath, querySourceStr)+".output",
			OpenNewFileFlags, 0644)
		if err != nil {
			return result, err
		}
		queryOutput = bufio.NewWriterSize(queryOutputFile, 8192)
		wgQueryOutput = &sync.WaitGroup{}
	}

	err = clientResult.Drain(ctx, func(qr *presto.QueryResults) error {
		result.RowCount += len(qr.Data)
		if queryOutput == nil {
			return nil
		}
		// Write output asynchronously. wgExitMainStage prevents the program from exiting if there is still
		// output data to write after all the queries finish running.
		s.States.wgExitMainStage.Add(1)
		// wgQueryOutput waits for all the writes to complete then flush and close the file.
		wgQueryOutput.Add(1)
		go func(data []json.RawMessage) {
			defer func() {
				wgQueryOutput.Done()
				s.States.wgExitMainStage.Done()
			}()
			for _, row := range data {
				_, ioErr := queryOutput.Write(row)
				if ioErr == nil {
					ioErr = queryOutput.WriteByte('\n')
				}
				if ioErr != nil {
					log.Error().Err(ioErr).EmbedObject(result.SimpleLogging()).
						Msg("failed to write query result")
					return
				}
			}
		}(qr.Data)
		if qr.NextUri == nil {
			_ = s.saveColumnMetadataFile(qr, result, querySourceStr)
		}
		return nil
	})

	// Now all the data for this query is being written
	if queryOutput != nil {
		s.States.wgExitMainStage.Add(1)
		go func() {
			wgQueryOutput.Wait()
			if ioErr := queryOutput.Flush(); ioErr != nil {
				log.Error().Err(ioErr).EmbedObject(result.SimpleLogging()).
					Msg("failed to write query result")
			} else {
				log.Debug().EmbedObject(result.SimpleLogging()).Msg("query data saved successfully")
			}
			_ = queryOutputFile.Close()
			s.States.wgExitMainStage.Done()
		}()
	}
	return result, err
}
