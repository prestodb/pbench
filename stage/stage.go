package stage

import (
	"context"
	"errors"
	"fmt"
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
	started atomic.Bool
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

func attachAdditionalInfoToQueryError(err error, query string, s *Stage) {
	var queryErr *presto.QueryError
	if errors.As(err, &queryErr) {
		if s != nil {
			queryErr.StageId = s.Id
		}
		queryErr.Query = query
	}
}

func (s *Stage) logCtxErr(ctx context.Context) {
	if ctx.Err() == nil {
		return
	}
	logEvent := log.Error().Str("benchmark_stage_id", s.Id).Err(ctx.Err())
	if cause := context.Cause(ctx); cause != nil {
		var queryError *presto.QueryError
		if errors.As(cause, &queryError) {
			logEvent.Str("caused_by_stage", queryError.StageId).
				Str("caused_by_query", queryError.QueryId).
				Str("info_url", queryError.InfoUrl)
		} else {
			logEvent.Str("caused_by_error", fmt.Sprint(ctx.Err()))
		}
	}
	logEvent.Msg("stage aborted")
}

// Run this stage and trigger its downstream stages.
func (s *Stage) Run(ctx context.Context) []error {
	errs, errChan := make([]error, 0, 2), make(chan error)
	wgExit := &sync.WaitGroup{}
	wgExit.Add(1)
	go func() {
		wgExit.Wait()
		close(errChan)
	}()

	ctx, s.AbortAll = context.WithCancelCause(ctx)
	log.Debug().Str("benchmark_stage_id", s.Id).Msg("created cancellable context")

	go func() {
		_ = s.run(ctx, errChan, wgExit)
	}()
	for err := range errChan {
		errs = append(errs, err)
	}
	return errs
}

func (s *Stage) run(ctx context.Context, errChan chan error, wgExitMainStage *sync.WaitGroup) (err error) {
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
				log.Error().Str("benchmark_stage_id", s.Id).
					Object("details", log.NewObjectMarshaller(err)).Msg("query failed")
				errChan <- err
			}
			if s.AbortOnError && s.AbortAll != nil {
				log.Debug().Str("benchmark_stage_id", s.Id).Msg("canceling the context because abort_on_error is set to true")
				s.AbortAll(err)
			}
		} else {
			// Trigger descendant stages.
			wgExitMainStage.Add(len(s.NextStages))
			for _, nextStage := range s.NextStages {
				go func(nextStage *Stage) {
					_ = nextStage.run(ctx, errChan, wgExitMainStage)
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
		log.Debug().Str("benchmark_stage_id", s.Id).Msg("all prerequisites finished")
	}
	if s.Client == nil || s.StartOnNewClient {
		if s.GetClient == nil {
			s.GetClient = DefaultGetClientFn
			log.Debug().Msg("using DefaultGetClientFn")
		}
		s.Client = s.GetClient()
		log.Debug().Str("benchmark_stage_id", s.Id).Msg("created new client")
	}
	if s.Catalog != nil {
		s.Client.Catalog(*s.Catalog)
		log.Debug().Str("benchmark_stage_id", s.Id).Str("catalog", *s.Catalog).Msg("set catalog")
	}
	if s.Schema != nil {
		s.Client.Schema(*s.Schema)
		log.Debug().Str("benchmark_stage_id", s.Id).Str("schema", *s.Schema).Msg("set schema")
	}
	for k, v := range s.SessionParams {
		s.Client.SessionParam(k, v)
	}
	if len(s.SessionParams) > 0 {
		log.Debug().Str("benchmark_stage_id", s.Id).
			Object("delta", log.NewMapMarshaller(s.SessionParams)).
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
			log.Error().Str("benchmark_stage_id", s.Id).Msgf("recovered from panic: %v", r)
		}
	}()
	logExecution := func() *zerolog.Event {
		event := log.Info().Str("benchmark_stage_id", s.Id)
		if filePath != nil {
			return event.Str("file", *filePath)
		}
		return event
	}
	for _, query := range queries {
		select {
		case <-ctx.Done():
			// context got cancelled, handle error and return.
			s.logCtxErr(ctx)
			return ctx.Err()
		default:
		}
		qr, _, err := s.Client.Query(ctx, query)
		if err != nil {
			attachAdditionalInfoToQueryError(err, query, s)
			return err
		}

		// Assemble log message
		e := logExecution().Str("query_id", qr.Id).Str("info_url", qr.InfoUri).
			Str("query", query)
		if catalog := s.Client.GetCatalog(); catalog != "" {
			e = e.Str("catalog", catalog)
		}
		if schema := s.Client.GetSchema(); schema != "" {
			e = e.Str("schema", schema)
		}
		e.Msgf("submitted query")

		rowCount, err := qr.Drain(ctx)
		if err != nil {
			attachAdditionalInfoToQueryError(err, query, s)
			return err
		}
		if s.OnQueryCompletion != nil {
			s.OnQueryCompletion(qr, rowCount)
		}
		logExecution().Str("query_id", qr.Id).Int("row_count", rowCount).Msgf("query finished")
	}
	return nil
}

func (s *Stage) MergeWith(other *Stage) {
	s.Id = other.Id
	if other.Catalog != nil {
		s.Catalog = other.Catalog
	}
	if other.Schema != nil {
		s.Schema = other.Schema
	}
	for k, v := range other.SessionParams {
		s.SessionParams[k] = v
	}
	s.Queries = append(s.Queries, other.Queries...)
	s.QueryFiles = append(s.QueryFiles, other.QueryFiles...)
	s.StartOnNewClient = other.StartOnNewClient
	s.AbortOnError = other.AbortOnError
	s.NextStagePaths = append(s.NextStagePaths, other.NextStagePaths...)
}
