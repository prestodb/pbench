package presto_benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"strings"
	"sync"
	"sync/atomic"
)

type GetClientFn func() *presto.Client

type Task struct {
	Id            string                  `json:"-"`
	Queries       []string                `json:"queries"`
	QueryFiles    []string                `json:"query_files"`
	NextIds       []string                `json:"next,omitempty"`
	AbortOnError  bool                    `json:"abort_on_error"`
	Prerequisites []*Task                 `json:"-"`
	Next          []*Task                 `json:"-"`
	Client        *presto.Client          `json:"-"`
	GetClient     GetClientFn             `json:"-"`
	AbortAll      context.CancelCauseFunc `json:"-"`
	wg            *sync.WaitGroup
	started       atomic.Bool
}

func (t *Task) waitForPrerequisites() <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		t.wg.Wait()
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
		log.Info().Str("query", query).Int("row_count", rowCount).Msgf("query execution completed")
	}
	return nil
}

// Run this task and trigger its downstream tasks.
func (t *Task) Run(ctx context.Context) error {
	if !t.started.CompareAndSwap(false, true) {
		// If other prerequisites finished earlier, then this task is already called and waiting.
		return nil
	}
	select {
	case <-ctx.Done():
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return ctx.Err()
	case <-t.waitForPrerequisites():
	}
	if t.Client == nil {
		if t.GetClient == nil {
			return fmt.Errorf("neither a Presto client nor a Presto client factory was provided")
		}
		t.Client = t.GetClient()
	}
	if t.AbortOnError && t.AbortAll == nil {
		ctx, t.AbortAll = context.WithCancelCause(ctx)
	}
	t.Client.AppendClientTag(t.Id)
	if err := t.runQueries(ctx, t.Queries); err != nil {
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
	for _, task := range t.Next {
		task.wg.Done()
		if task.GetClient == nil {
			task.GetClient = t.GetClient
		}
		if task.Client == nil {
			task.Client = t.Client
		}
		go func(task *Task) {
			err := task.Run(ctx)
			if err == nil {
				return
			}
			log.Error().Err(err).Send()
			if task.AbortOnError {
				task.AbortAll(err)
			}
		}(task)
	}
	return nil
}

type TaskMap map[string]*Task

const (
	RootTaskId = "__root__"
	TaskDefExt = ".json"
)

func ParseDirectory(dirPath string) (TaskMap, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	tasks := make(TaskMap)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), TaskDefExt) {
			filePath := path.Join(dirPath, entry.Name())
			bytes, err := os.ReadFile(filePath)
			if err != nil {
				log.Printf("failed to read %s: %v, skipping\n", filePath, err)
				continue
			}
			taskId := strings.TrimSuffix(entry.Name(), TaskDefExt)
			task := &Task{
				Id: taskId,
			}
			if err = json.Unmarshal(bytes, task); err != nil {
				log.Printf("failed to parse json %s: %v, skipping\n", filePath, err)
			} else {
				tasks[taskId] = task
				for _, queryFile := range task.QueryFiles {
					if !filepath.IsAbs(queryFile) {
						queryFile = filepath.Join(dirPath, queryFile)
					}
					if _, err := os.Stat(queryFile); err != nil {
						log.Printf("%s links to an invalid query file %s: %v\n", taskId, queryFile, err)
					}
				}
			}
		}
	}
	tasks.Link()
	return tasks, nil
}

func (tm *TaskMap) Link() {
	root := &Task{}
	for taskId, task := range *tm {
		nextIds := make(map[string]bool)
		if len(task.Next) > 0 {
			task.Next = nil // clear pre-existing data, if there is any
		}
		for _, nextId := range task.NextIds {
			if nextId == taskId {
				log.Fatal().Msgf("cyclic reference in %s.next", taskId)
			}
			if nextIds[nextId] {
				log.Fatal().Msgf("%s.next got duplicated id %s", taskId, nextId)
			}
			if nextTask, exists := (*tm)[nextId]; exists {
				task.Next = append(task.Next, nextTask)
				nextTask.Prerequisites = append(nextTask.Prerequisites, task)
				nextTask.wg.Add(1)
				nextIds[nextId] = true
			} else {
				log.Fatal().Msgf("%s.next %s is not found", taskId, nextId)
			}
		}
	}
	for _, task := range *tm {
		if len(task.Prerequisites) == 0 {
			root.Next = append(root.Next, task)
			task.Prerequisites = append(task.Prerequisites, root)
		}
	}
	(*tm)[RootTaskId] = root
}

func (tm *TaskMap) Root() *Task {
	if tm == nil {
		return nil
	}
	return (*tm)[RootTaskId]
}

func (tm *TaskMap) Get(taskId string) *Task {
	if tm == nil {
		return nil
	}
	return (*tm)[taskId]
}
