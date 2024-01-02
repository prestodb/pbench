package stage

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"os/signal"
	"presto-benchmark/log"
	"presto-benchmark/presto"
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
	// started is used to make sure only one goroutine is started to run this stage when there are multiple prerequisites.
	started    atomic.Bool
	resultChan chan *QueryResult
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
		log.Debug().EmbedObject(s).Msg("created new client")
	}
	if s.Catalog != nil {
		s.Client.Catalog(*s.Catalog)
		log.Debug().EmbedObject(s).Str("catalog", *s.Catalog).Msg("set catalog")
	}
	if s.Schema != nil {
		s.Client.Schema(*s.Schema)
		log.Debug().EmbedObject(s).Str("schema", *s.Schema).Msg("set schema")
	}
	for k, v := range s.SessionParams {
		s.Client.SessionParam(k, v)
	}
	if len(s.SessionParams) > 0 {
		log.Debug().EmbedObject(s).
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
		if nextStage.resultChan == nil {
			nextStage.resultChan = s.resultChan
		}
		if nextStage.SaveData == nil {
			nextStage.SaveData = s.SaveData
		}
		if nextStage.SaveJson == nil {
			nextStage.SaveJson = s.SaveJson
		}
	}
}

// Run this stage and trigger its downstream stages.
func (s *Stage) Run(ctx context.Context) []*QueryResult {
	results := make([]*QueryResult, 0, len(s.Queries)+len(s.QueryFiles))
	s.resultChan = make(chan *QueryResult)
	wgExitMainStage := &sync.WaitGroup{}
	wgExitMainStage.Add(1)
	go func() {
		wgExitMainStage.Wait()
		close(s.resultChan)
	}()

	ctx, s.AbortAll = context.WithCancelCause(ctx)
	log.Debug().EmbedObject(s).Msg("created cancellable context")

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	go func() {
		sig := <-sigint
		s.AbortAll(fmt.Errorf(sig.String()))
		signal.Stop(sigint)
	}()
	go func() {
		_ = s.run(ctx, wgExitMainStage)
	}()
	for result := range s.resultChan {
		results = append(results, result)
	}
	return results
}

func (s *Stage) run(ctx context.Context, wgExitMainStage *sync.WaitGroup) (err error) {
	if !s.started.CompareAndSwap(false, true) {
		// If other prerequisites finished earlier, then this stage is already called and waiting.
		wgExitMainStage.Done()
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
			wgExitMainStage.Add(len(s.NextStages))
			for _, nextStage := range s.NextStages {
				go func(nextStage *Stage) {
					_ = nextStage.run(ctx, wgExitMainStage)
				}(nextStage)
			}
		}
		wgExitMainStage.Done()
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

func (s *Stage) runQueries(ctx context.Context, queries []string, queryFile *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().EmbedObject(s).Msgf("recovered from panic: %v", r)
			if e, ok := r.(error); ok {
				err = e
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
		qr, _, queryErr := s.Client.Query(ctx, query)
		if qr != nil {
			result.QueryId = qr.Id
			result.InfoUrl = qr.InfoUri
		}
		if queryErr != nil {
			result.QueryError = queryErr
			result.ConcludeExecution()
			if s.OnQueryCompletion != nil {
				s.OnQueryCompletion(result)
			}
			// Each query should have a query result sent to the channel, no matter
			// its execution succeeded or not.
			s.resultChan <- result
			if errors.Is(queryErr, context.Canceled) || errors.Is(queryErr, context.DeadlineExceeded) {
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
		e := log.Info().EmbedObject(result)
		if catalog := s.Client.GetCatalog(); catalog != "" {
			e = e.Str("catalog", catalog)
		}
		if schema := s.Client.GetSchema(); schema != "" {
			e = e.Str("schema", schema)
		}
		e.Msgf("submitted query")

		queryErr = qr.Drain(ctx, func(qr *presto.QueryResults) {
			result.RowCount += len(qr.Data)
			if *s.SaveData {
				// TODO: save data
			}
		})
		result.QueryError = queryErr
		result.ConcludeExecution()
		if s.OnQueryCompletion != nil {
			s.OnQueryCompletion(result)
		}
		// Each query should have a query result sent to the channel, no matter
		// its execution succeeded or not.
		s.resultChan <- result
		if queryErr != nil {
			if errors.Is(queryErr, context.Canceled) || errors.Is(queryErr, context.DeadlineExceeded) {
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
		log.Info().EmbedObject(result.NoLoggingQuery()).Msgf("query finished")
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
