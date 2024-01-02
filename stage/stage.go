package stage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
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

type GetClientFn func() *presto.Client
type OnQueryCompletionFn func(result *QueryResult)

var DefaultServerUrl = "http://127.0.0.1:8080"
var DefaultGetClientFn = func() *presto.Client {
	client, _ := presto.NewClient(DefaultServerUrl)
	return client
}

type Stage struct {
	// Id is used to uniquely identify a stage. It is usually the file name without its directory path and extension.
	Id string `json:"-"`
	// Catalog, schema, and session params will be inherited by the children stages unless a new client is created
	// by setting start_on_new_client = true on children stages.
	Catalog       *string        `json:"catalog,omitempty"`
	Schema        *string        `json:"schema,omitempty"`
	SessionParams map[string]any `json:"session_params,omitempty"`
	Queries       []string       `json:"queries,omitempty"`
	// If a stage has both Queries and QueryFiles, the queries in the Queries array will be executed first then
	// the QueryFiles will be read and executed.
	QueryFiles []string `json:"query_files,omitempty"`
	// If StartOnNewClient is set to true, this stage will create a new client to execute itself.\
	// This new client will be passed down to its descendant stages unless those stages also set StartOnNewClient to true.
	// Each client can carry their own set of client information, tags, session properties, user credentials, etc.
	// Descendant stages will **NOT** inherit this value from their predecessors.
	StartOnNewClient bool `json:"start_on_new_client,omitempty"`
	// If AbortOnError is set to true, the context associated with this stage will be canceled if an error occurs.
	// Depending on when the cancellable context was created, this may abort some or all other running stages and all future stages.
	// Children stages will inherit this value from their parent if it is not set.
	AbortOnError *bool `json:"abort_on_error,omitempty"`
	// If SaveData is set to true, the query result will be saved to files in its raw form.
	// Children stages will inherit this value from their parent if it is not set.
	SaveData *bool `json:"save_data,omitempty"`
	// If SaveJson is set to true, the query json will be saved to files in its raw form.
	// Children stages will inherit this value from their parent if it is not set.
	SaveJson       *bool    `json:"save_json,omitempty"`
	NextStagePaths []string `json:"next,omitempty"`
	BaseDir        string   `json:"-"`
	Prerequisites  []*Stage `json:"-"`
	NextStages     []*Stage `json:"-"`
	// Client is by default passed down to descendant stages.
	Client *presto.Client `json:"-"`
	// GetClient is called when the stage needs to create a new Presto client. This function is passed down to descendant stages by default.
	GetClient GetClientFn `json:"-"`
	// AbortAll is passed down to descendant stages by default and will be used to cancel the current context.
	AbortAll          context.CancelCauseFunc `json:"-"`
	OnQueryCompletion OnQueryCompletionFn     `json:"-"`
	// wgPrerequisites is a count-down latch to wait for all the prerequisites to finish before starting this stage.
	wgPrerequisites sync.WaitGroup
	wgExitMainStage *sync.WaitGroup
	// started is used to make sure only one goroutine is started to run this stage when there are multiple prerequisites.
	started    atomic.Bool
	resultChan chan *QueryResult
	outputPath string
}

func (s *Stage) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", s.Id)
}

func (s *Stage) String() string {
	return s.Id
}

func (s *Stage) waitForPrerequisites() <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		s.wgPrerequisites.Wait()
		close(ch)
	}()
	return ch
}

func (s *Stage) logErr(ctx context.Context, err error) {
	var queryResult *QueryResult
	logEvent := log.Error()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		logEvent.EmbedObject(s)
		if cause := context.Cause(ctx); cause != nil && errors.As(cause, &queryResult) {
			logEvent.Str("caused_by_stage", queryResult.StageId).
				Str("caused_by_query", queryResult.QueryId).
				Str("info_url", queryResult.InfoUrl)
		} else {
			logEvent.AnErr("caused_by_error", err)
		}
		logEvent.Msg("stage aborted")
		return
	}
	if errors.As(err, &queryResult) {
		logEvent.EmbedObject(queryResult)
	} else {
		logEvent.EmbedObject(s).EmbedObject(log.NewMarshaller(err))
	}
	logEvent.Msg("query failed")
}

func (s *Stage) prepareClient() {
	if s.Client == nil || s.StartOnNewClient {
		if s.GetClient == nil {
			s.GetClient = DefaultGetClientFn
			log.Debug().Msg("using DefaultGetClientFn")
		}
		s.Client = s.GetClient()
		log.Info().EmbedObject(s).Msg("created new client")
	}
	if s.Catalog != nil {
		s.Client.Catalog(*s.Catalog)
		log.Info().EmbedObject(s).Str("catalog", *s.Catalog).Msg("set catalog")
	}
	if s.Schema != nil {
		s.Client.Schema(*s.Schema)
		log.Info().EmbedObject(s).Str("schema", *s.Schema).Msg("set schema")
	}
	for k, v := range s.SessionParams {
		s.Client.SessionParam(k, v)
	}
	if len(s.SessionParams) > 0 {
		log.Info().EmbedObject(s).
			Object("delta", log.NewMarshaller(s.SessionParams)).
			Str("final", s.Client.GetSessionParams()).
			Msg("added session params")
	}
	s.Client.AppendClientTag(s.Id)
}

func (s *Stage) propagateStates() {
	falseValue := false
	if s.SaveJson == nil {
		s.SaveJson = &falseValue
	}
	if s.SaveData == nil {
		s.SaveData = &falseValue
	}
	if s.AbortOnError == nil {
		s.AbortOnError = &falseValue
	}
	for _, nextStage := range s.NextStages {
		if nextStage.GetClient == nil {
			nextStage.GetClient = s.GetClient
		}
		if nextStage.Client == nil {
			nextStage.Client = s.Client
		}
		if nextStage.AbortOnError == nil {
			nextStage.AbortOnError = s.AbortOnError
		}
		if nextStage.AbortAll == nil {
			nextStage.AbortAll = s.AbortAll
		}
		if nextStage.OnQueryCompletion == nil {
			nextStage.OnQueryCompletion = s.OnQueryCompletion
		}
		if nextStage.SaveData == nil {
			nextStage.SaveData = s.SaveData
		}
		if nextStage.SaveJson == nil {
			nextStage.SaveJson = s.SaveJson
		}
		nextStage.resultChan = s.resultChan
		nextStage.outputPath = s.outputPath
		nextStage.wgExitMainStage = s.wgExitMainStage
	}
}

// Run this stage and trigger its downstream stages.
func (s *Stage) Run(ctx context.Context) []*QueryResult {
	results := make([]*QueryResult, 0, len(s.Queries)+len(s.QueryFiles))
	s.resultChan = make(chan *QueryResult)
	s.wgExitMainStage = &sync.WaitGroup{}
	s.wgExitMainStage.Add(1)
	ctx, s.AbortAll = context.WithCancelCause(ctx)
	log.Debug().EmbedObject(s).Msg("created cancellable context")

	// create output directory
	s.outputPath = filepath.Join(s.BaseDir, fmt.Sprintf("%s-output-%s", s.Id, time.Now().Format(time.RFC3339)))
	if err := os.Mkdir(s.outputPath, 0755); err != nil {
		log.Fatal().Str("output_path", s.outputPath).
			Err(err).Msg("failed to create output directory")
	} else {
		log.Info().Str("output_path", s.outputPath).
			Msg("output will be saved in this path")
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	go func() {
		sig := <-sigint
		s.AbortAll(fmt.Errorf(sig.String()))
		signal.Stop(sigint)
	}()
	go func() {
		s.wgExitMainStage.Wait()
		close(s.resultChan)
	}()
	go func() {
		_ = s.run(ctx)
	}()
	b := strings.Builder{}
	b.WriteString("stage_id,query_file,query_index,info_url,succeeded,row_count,start_time,end_time,duration\n")
	for result := range s.resultChan {
		results = append(results, result)
		b.WriteString(result.StageId + ",")
		if result.QueryFile != nil {
			b.WriteString(*result.QueryFile)
		} else {
			b.WriteString("inline")
		}
		b.WriteString(fmt.Sprintf(",%d,%s,%t,%d,%s,%s,%s\n",
			result.QueryIndex, result.InfoUrl, result.QueryError == nil, result.RowCount,
			result.StartTime.Format(time.RFC3339), result.EndTime.Format(time.RFC3339), *result.Duration))
	}
	_ = os.WriteFile(filepath.Join(s.outputPath, "summary.csv"), []byte(b.String()), 0644)
	return results
}

func (s *Stage) run(ctx context.Context) (err error) {
	if !s.started.CompareAndSwap(false, true) {
		// If other prerequisites finished earlier, then this stage is already called and waiting.
		s.wgExitMainStage.Done()
		return nil
	}
	defer func() {
		for _, nextStage := range s.NextStages {
			nextStage.wgPrerequisites.Done()
		}
		if err != nil {
			s.logErr(ctx, err)
			if *s.AbortOnError && s.AbortAll != nil {
				log.Debug().EmbedObject(s).Msg("canceling the context because abort_on_error is set to true")
				s.AbortAll(err)
			}
		} else {
			// Trigger descendant stages.
			s.wgExitMainStage.Add(len(s.NextStages))
			for _, nextStage := range s.NextStages {
				go func(nextStage *Stage) {
					_ = nextStage.run(ctx)
				}(nextStage)
			}
		}
		s.wgExitMainStage.Done()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.waitForPrerequisites():
		log.Debug().EmbedObject(s).Msg("all prerequisites finished")
	}
	s.prepareClient()
	s.propagateStates()
	if err = s.runQueries(ctx, s.Queries, nil); err != nil {
		return err
	}
	for _, filePath := range s.QueryFiles {
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		queries, err := presto.SplitQueries(file)
		if err != nil {
			return err
		}
		err = s.runQueries(ctx, queries, &filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func getNow() *time.Time {
	now := time.Now()
	return &now
}

func queryOutputFileName(s *Stage, result *QueryResult) (fileName string) {
	if result.QueryFile != nil {
		fileName = fileNameWithoutPathAndExt(*result.QueryFile)
	} else {
		fileName = "inline"
	}
	fileName = fmt.Sprintf("%s_%s_q%d", s.Id, fileName, result.QueryIndex)
	return
}

func (s *Stage) runQueries(ctx context.Context, queries []string, queryFile *string) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().EmbedObject(s).Msgf("recovered from panic: %v", r)
			if e, ok := r.(error); ok {
				retErr = e
			}
		}
	}()
	for i, query := range queries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		result := &QueryResult{
			StageId:    s.Id,
			Query:      query,
			QueryFile:  queryFile,
			QueryIndex: i,
			QueryRows:  make([]presto.QueryRow, 0),
			StartTime:  time.Now(),
		}
		qr, _, err := s.Client.Query(ctx, query)
		if qr != nil {
			result.QueryId = qr.Id
			result.InfoUrl = qr.InfoUri
		}
		if err != nil {
			result.QueryError = err
			result.ConcludeExecution()
			if s.OnQueryCompletion != nil {
				s.OnQueryCompletion(result)
			}
			// Each query should have a query result sent to the channel, no matter
			// its execution succeeded or not.
			s.resultChan <- result
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// If the context is cancelled or timed out, we cannot continue whatsoever and must return.
				return result
			}
			if *s.AbortOnError {
				// Skip the rest queries in the same batch.
				// Logging etc. will be handled in the parent stack.
				return result
			}
			// Log the error information and continue running
			s.logErr(ctx, result)
			continue
		}

		// Log query submission
		e := log.Debug().EmbedObject(result.SimpleLogging())
		if catalog := s.Client.GetCatalog(); catalog != "" {
			e = e.Str("catalog", catalog)
		}
		if schema := s.Client.GetSchema(); schema != "" {
			e = e.Str("schema", schema)
		}
		if *s.SaveData {
			e.Bool("save_data", true)
		}
		e.Msgf("submitted query")

		var (
			queryOutputFile *os.File
			queryOutput     *bufio.Writer
			wgQueryOutput   *sync.WaitGroup
		)
		if *s.SaveData {
			queryOutputFile, err = os.OpenFile(
				filepath.Join(s.outputPath, queryOutputFileName(s, result))+".output",
				os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			if err == nil {
				queryOutput = bufio.NewWriterSize(queryOutputFile, 8192)
				wgQueryOutput = &sync.WaitGroup{}
			}
		}
		if err == nil {
			err = qr.Drain(ctx, func(qr *presto.QueryResults) error {
				result.RowCount += len(qr.Data)
				if queryOutput == nil {
					return nil
				}
				// Write output asynchronously. wgExitMainStage prevents the program from exiting if there is still
				// output data to write after all the queries finish running.
				s.wgExitMainStage.Add(1)
				// wgQueryOutput waits for all the writes to complete then flush and close the file.
				wgQueryOutput.Add(1)
				go func(data []json.RawMessage) {
					defer func() {
						wgQueryOutput.Done()
						s.wgExitMainStage.Done()
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
				return nil
			})
			// Now all the data for this query is being written
			if queryOutput != nil {
				s.wgExitMainStage.Add(1)
				go func() {
					wgQueryOutput.Wait()
					if ioErr := queryOutput.Flush(); ioErr != nil {
						log.Error().Err(ioErr).EmbedObject(result.SimpleLogging()).
							Msg("failed to write query result")
					}
					_ = queryOutputFile.Close()
					log.Debug().EmbedObject(result.SimpleLogging()).Msg("query data saved successfully")
					s.wgExitMainStage.Done()
				}()
			}
		}
		result.QueryError = err
		result.ConcludeExecution()
		if s.OnQueryCompletion != nil {
			s.OnQueryCompletion(result)
		}
		// Each query should have a query result sent to the channel, no matter
		// its execution succeeded or not.
		s.resultChan <- result
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// If the context is cancelled or timed out, we cannot continue whatsoever and must return.
				return result
			}
			if *s.AbortOnError {
				// Skip the rest queries in the same batch.
				// Logging etc. will be handled in the parent stack.
				return result
			}
			// Log the error information and continue running
			s.logErr(ctx, result)
			continue
		}
		log.Info().EmbedObject(result).Msgf("query finished")
	}
	return nil
}

func (s *Stage) MergeWith(other *Stage) *Stage {
	s.Id = other.Id
	if other.Catalog != nil {
		s.Catalog = other.Catalog
	}
	if other.Schema != nil {
		s.Schema = other.Schema
	}
	if s.SessionParams == nil {
		s.SessionParams = make(map[string]any)
	}
	for k, v := range other.SessionParams {
		s.SessionParams[k] = v
	}
	s.Queries = append(s.Queries, other.Queries...)
	s.QueryFiles = append(s.QueryFiles, other.QueryFiles...)
	s.StartOnNewClient = other.StartOnNewClient
	s.AbortOnError = other.AbortOnError
	s.NextStagePaths = append(s.NextStagePaths, other.NextStagePaths...)
	s.BaseDir = other.BaseDir
	s.SaveData = other.SaveData
	return s
}
