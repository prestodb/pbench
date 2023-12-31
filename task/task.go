package task

import (
	"context"
	"os"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"sync"
	"sync/atomic"
)

type GetClientFn func() *presto.Client

var DefaultServerUrl = "http://127.0.0.1:8080"
var DefaultGetClientFn = func() *presto.Client {
	client, _ := presto.NewClient(DefaultServerUrl)
	return client
}

type Task struct {
	// Id is used to uniquely identify a task. It is usually the file name without its directory path and extension.
	Id            string         `json:"-"`
	Catalog       *string        `json:"catalog,omitempty"`
	Schema        *string        `json:"schema,omitempty"`
	SessionParams map[string]any `json:"session_params,omitempty"`
	Queries       []string       `json:"queries,omitempty"`
	// If a task has both Queries and QueryFiles, the queries in the Queries array will be executed first then
	// the QueryFiles will be read and executed.
	QueryFiles []string `json:"query_files,omitempty"`
	// If StartOnNewClient is set to true, this task will create a new client to execute itself.\
	// This new client will be passed down to its descendant tasks unless those tasks also set StartOnNewClient to true.
	// Each client can carry their own set of client information, tags, session properties, user credentials, etc.
	StartOnNewClient bool `json:"start_on_new_client,omitempty"`
	// If AbortOnError is set to true, the context associated with this task will be canceled if an error occurs.
	// Depending on when the cancellable context was created, this may abort some or all other running tasks and all future tasks.
	AbortOnError  bool     `json:"abort_on_error,omitempty"`
	NextTaskPaths []string `json:"next,omitempty"`
	Prerequisites []*Task  `json:"-"`
	NextTasks     []*Task  `json:"-"`
	// Client is by default passed down to descendant tasks.
	Client *presto.Client `json:"-"`
	// GetClient is called when the task needs to create a new Presto client. This function is passed down to descendant tasks by default.
	GetClient GetClientFn `json:"-"`
	// AbortAll is passed down to descendant tasks by default and will be used to cancel the current context.
	AbortAll context.CancelCauseFunc `json:"-"`
	// wgPrerequisites is a count-down latch to wait for all the prerequisites to finish before starting this task.
	wgPrerequisites sync.WaitGroup
	// started is used to make sure only one goroutine is started to run this task when there are multiple prerequisites.
	started atomic.Bool
}

func (t *Task) waitForPrerequisites() <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		t.wgPrerequisites.Wait()
		close(ch)
	}()
	return ch
}

func (t *Task) runQueries(ctx context.Context, queries []string) error {
	for _, query := range queries {
		log.Info().Str("query", query).Msgf("start to execute query")
		qr, _, err := t.Client.Query(ctx, query)
		if err != nil {
			return err
		}
		rowCount, err := qr.Drain(ctx)
		if err != nil {
			return err
		}
		log.Info().Str("query", query).Int("row_count", rowCount).Msgf("query finished")
	}
	return nil
}

// Run this task and trigger its downstream tasks.
func (t *Task) Run(ctx context.Context) (err error) {
	if !t.started.CompareAndSwap(false, true) {
		// If other prerequisites finished earlier, then this task is already called and waiting.
		return nil
	}
	defer func() {
		for _, task := range t.NextTasks {
			task.wgPrerequisites.Done()
		}
		if err != nil && t.AbortOnError && t.AbortAll != nil {
			t.AbortAll(err)
		}
	}()
	select {
	case <-ctx.Done():
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return ctx.Err()
	case <-t.waitForPrerequisites():
	}
	if t.Client == nil || t.StartOnNewClient {
		if t.GetClient == nil {
			t.GetClient = DefaultGetClientFn
		}
		t.Client = t.GetClient()
	}
	if t.Catalog != nil {
		t.Client.Catalog(*t.Catalog)
	}
	if t.Schema != nil {
		t.Client.Schema(*t.Schema)
	}
	for k, v := range t.SessionParams {
		t.Client.SessionParam(k, v)
	}
	if t.AbortOnError && t.AbortAll == nil {
		// This ctx will be passed down to descendant tasks.
		ctx, t.AbortAll = context.WithCancelCause(ctx)
	}
	for _, task := range t.NextTasks {
		if task.GetClient == nil {
			task.GetClient = t.GetClient
		}
		if task.Client == nil {
			task.Client = t.Client
		}
		if task.AbortAll == nil {
			task.AbortAll = t.AbortAll
		}
	}
	t.Client.AppendClientTag(t.Id)
	if err = t.runQueries(ctx, t.Queries); err != nil {
		return err
	}
	for _, filePath := range t.QueryFiles {
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}
		queries, err := presto.SplitQueries(file)
		if err != nil {
			return err
		}
		err = t.runQueries(ctx, queries)
		if err != nil {
			return err
		}
	}
	for _, task := range t.NextTasks {
		go func(task *Task) {
			err := task.Run(ctx)
			if err != nil {
				log.Error().Err(err).Send()
			}
		}(task)
	}
	return nil
}
