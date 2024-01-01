package stage

import (
	"context"
	"errors"
	"github.com/rs/zerolog"
	"os"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"sync"
	"sync/atomic"
)

type GetClientFn func() *presto.Client
type OnQueryCompletionFn func(qr *presto.QueryResults, rowCount int)

var DefaultServerUrl = "http://127.0.0.1:8080"
var DefaultGetClientFn = func() *presto.Client {
	client, _ := presto.NewClient(DefaultServerUrl)
	return client
}

type Stage struct {
	// Id is used to uniquely identify a stage. It is usually the file name without its directory path and extension.
	Id            string         `json:"-"`
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
	StartOnNewClient bool `json:"start_on_new_client,omitempty"`
	// If AbortOnError is set to true, the context associated with this stage will be canceled if an error occurs.
	// Depending on when the cancellable context was created, this may abort some or all other running stages and all future stages.
	AbortOnError   bool     `json:"abort_on_error,omitempty"`
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
	started   atomic.Bool
	errorChan chan error
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

func (s *Stage) attachAdditionalInfoToQueryResults(qr *presto.QueryResults, query string, filePath *string, queryIndex int) {
	qr.StageId = s.Id
	if filePath == nil {
		qr.Query = &query
	} else {
		qr.QueryFile = filePath
	}
	qr.QueryIndex = queryIndex
	if qr.Error != nil {
		qr.Error.QueryMetadata = &qr.QueryMetadata
	}
}

func (s *Stage) logStageId(e *zerolog.Event) *zerolog.Event {
	return e.Str("benchmark_stage_id", s.Id)
}

func (s *Stage) logCtxErr(ctx context.Context) {
	if ctx.Err() == nil {
		return
	}
	logEvent := s.logStageId(log.Error()).Err(ctx.Err())
	if cause := context.Cause(ctx); cause != nil {
		var queryError *presto.QueryError
		if errors.As(cause, &queryError) {
			logEvent.Str("caused_by_stage", queryError.StageId).
				Str("caused_by_query", *queryError.QueryId).
				Str("info_url", *queryError.InfoUrl)
		} else {
			logEvent.AnErr("caused_by_error", ctx.Err())
		}
	}
	logEvent.Msg("stage aborted")
}

// Run this stage and trigger its downstream stages.
func (s *Stage) Run(ctx context.Context) []error {
	errs := make([]error, 0, 2)
	s.errorChan = make(chan error)
	wgExit := &sync.WaitGroup{}
	wgExit.Add(1)
	go func() {
		wgExit.Wait()
		close(s.errorChan)
	}()

	ctx, s.AbortAll = context.WithCancelCause(ctx)
	s.logStageId(log.Debug()).Msg("created cancellable context")

	go func() {
		_ = s.run(ctx, wgExit)
	}()
	for err := range s.errorChan {
		errs = append(errs, err)
	}
	return errs
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
			if !errors.Is(err, context.Canceled) {
				s.logStageId(log.Error()).Object("details", log.NewMarshaller(err)).Msg("query failed")
				s.errorChan <- err
			}
			if s.AbortOnError && s.AbortAll != nil {
				s.logStageId(log.Debug()).Msg("canceling the context because abort_on_error is set to true")
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
		s.logCtxErr(ctx)
		return ctx.Err()
	case <-s.waitForPrerequisites():
		s.logStageId(log.Debug()).Msg("all prerequisites finished")
	}
	if s.Client == nil || s.StartOnNewClient {
		if s.GetClient == nil {
			s.GetClient = DefaultGetClientFn
			log.Debug().Msg("using DefaultGetClientFn")
		}
		s.Client = s.GetClient()
		s.logStageId(log.Debug()).Msg("created new client")
	}
	if s.Catalog != nil {
		s.Client.Catalog(*s.Catalog)
		s.logStageId(log.Debug()).Str("catalog", *s.Catalog).Msg("set catalog")
	}
	if s.Schema != nil {
		s.Client.Schema(*s.Schema)
		s.logStageId(log.Debug()).Str("schema", *s.Schema).Msg("set schema")
	}
	for k, v := range s.SessionParams {
		s.Client.SessionParam(k, v)
	}
	if len(s.SessionParams) > 0 {
		s.logStageId(log.Debug()).
			Object("delta", log.NewMarshaller(s.SessionParams)).
			Str("final", s.Client.GetSessionParams()).
			Msg("added session params")
	}
	for _, nextStage := range s.NextStages {
		if nextStage.GetClient == nil {
			nextStage.GetClient = s.GetClient
		}
		if nextStage.Client == nil {
			nextStage.Client = s.Client
		}
		if nextStage.AbortAll == nil {
			nextStage.AbortAll = s.AbortAll
		}
		if nextStage.OnQueryCompletion == nil {
			nextStage.OnQueryCompletion = s.OnQueryCompletion
		}
		if nextStage.errorChan == nil {
			nextStage.errorChan = s.errorChan
		}
	}
	s.Client.AppendClientTag(s.Id)
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

func (s *Stage) runQueries(ctx context.Context, queries []string, filePath *string) error {
	defer func() {
		if r := recover(); r != nil {
			s.logStageId(log.Error()).Msgf("recovered from panic: %v", r)
		}
	}()
	for i, query := range queries {
		select {
		case <-ctx.Done():
			// context got cancelled, handle error and return.
			s.logCtxErr(ctx)
			return ctx.Err()
		default:
		}
		qr, _, err := s.Client.Query(ctx, query)
		s.attachAdditionalInfoToQueryResults(qr, query, filePath, i)
		if err != nil {
			if !s.AbortOnError {
				if !errors.Is(err, context.Canceled) {
					s.errorChan <- err
					s.logStageId(log.Error()).Object("details", log.NewMarshaller(err)).Msg("query failed")
				}
				continue
			}
			return err
		}

		// Assemble log message
		e := s.logStageId(log.Info()).
			Int("query_index", i).
			Str("query_id", qr.Id).
			Str("info_url", qr.InfoUri)
		if filePath != nil {
			e = e.Str("query_file", *filePath)
		} else {
			e = e.Str("query", query)
		}
		if catalog := s.Client.GetCatalog(); catalog != "" {
			e = e.Str("catalog", catalog)
		}
		if schema := s.Client.GetSchema(); schema != "" {
			e = e.Str("schema", schema)
		}
		e.Msgf("submitted query")

		rowCount, err := qr.Drain(ctx)
		s.attachAdditionalInfoToQueryResults(qr, query, filePath, i)
		if err != nil {
			if !s.AbortOnError {
				if !errors.Is(err, context.Canceled) {
					s.errorChan <- err
					s.logStageId(log.Error()).Object("details", log.NewMarshaller(err)).Msg("query failed")
				}
				continue
			}
			return err
		}
		if s.OnQueryCompletion != nil {
			s.OnQueryCompletion(qr, rowCount)
		}
		e = s.logStageId(log.Info()).
			Int("query_index", i).
			Str("query_id", qr.Id).
			Int("row_count", rowCount)
		if filePath != nil {
			e = e.Str("query_file", *filePath)
		}
		e.Msgf("query finished")
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
	return s
}
