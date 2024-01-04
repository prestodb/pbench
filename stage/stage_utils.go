package stage

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"presto-benchmark/log"
	"time"
)

func getNow() *time.Time {
	now := time.Now()
	return &now
}

func querySource(s *Stage, result *QueryResult) (fileName string) {
	if result.QueryFile != nil {
		fileName = fileNameWithoutPathAndExt(*result.QueryFile)
	} else {
		fileName = "inline"
	}
	fileName = fmt.Sprintf("%s_%s_q%d", s.Id, fileName, result.QueryIndex)
	return
}

func (s *Stage) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", s.Id)
}

func (s *Stage) String() string {
	return s.Id
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
	if s.SaveOutput == nil {
		s.SaveOutput = &falseValue
	}
	if s.AbortOnError == nil {
		s.AbortOnError = &falseValue
	}
	for _, nextStage := range s.NextStages {
		if nextStage.Catalog == nil {
			nextStage.Catalog = s.Catalog
		}
		if nextStage.Schema == nil {
			nextStage.Schema = s.Schema
		}
		if nextStage.SessionParams == nil {
			nextStage.SessionParams = make(map[string]any)
		}
		for k, v := range s.SessionParams {
			if _, ok := nextStage.SessionParams[k]; !ok {
				nextStage.SessionParams[k] = v
			}
		}
		if nextStage.AbortOnError == nil {
			nextStage.AbortOnError = s.AbortOnError
		}
		if nextStage.SaveOutput == nil {
			nextStage.SaveOutput = s.SaveOutput
		}
		if nextStage.SaveJson == nil {
			nextStage.SaveJson = s.SaveJson
		}
		nextStage.OutputPath = s.OutputPath
		if nextStage.GetClient == nil {
			nextStage.GetClient = s.GetClient
			if nextStage.Client == nil {
				// If the next stage didn't have a getClient function and a client before,
				// then we just let it use ours.
				nextStage.Client = s.Client
			}
		}
		nextStage.AbortAll = s.AbortAll
		nextStage.OnQueryCompletion = s.OnQueryCompletion
		nextStage.wgExitMainStage = s.wgExitMainStage
		nextStage.resultChan = s.resultChan
	}
}

func (s *Stage) MergeWith(other *Stage) *Stage {
	if s.Id == "" {
		s.Id = other.Id
	} else {
		s.Id += "_" + other.Id
	}
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
	if other.AbortOnError != nil {
		s.AbortOnError = other.AbortOnError
	}
	if other.SaveOutput != nil {
		s.SaveOutput = other.SaveOutput
	}
	if other.SaveJson != nil {
		s.SaveJson = other.SaveJson
	}
	if s.OutputPath == "" {
		s.OutputPath = other.OutputPath
	}
	s.NextStagePaths = append(s.NextStagePaths, other.NextStagePaths...)
	s.BaseDir = other.BaseDir
	return s
}
