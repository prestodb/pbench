package presto_benchmark

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

type Task struct {
	Queries       []string        `json:"queries"`
	QueryFiles    []string        `json:"query_files"`
	NextIds       []string        `json:"next,omitempty"`
	Prerequisites []*Task         `json:"-"`
	Next          []*Task         `json:"-"`
	wg            *sync.WaitGroup `json:"-"`
}

type TaskMap map[string]*Task

const RootTaskId = "__root__"

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
		if strings.HasSuffix(entry.Name(), ".json") {
			filePath := path.Join(dirPath, entry.Name())
			bytes, err := os.ReadFile(filePath)
			if err != nil {
				log.Printf("failed to read %s: %v, skipping\n", filePath, err)
				continue
			}
			task := &Task{}
			if err = json.Unmarshal(bytes, task); err != nil {
				log.Printf("failed to parse json %s: %v, skipping\n", filePath, err)
			} else {
				taskId := strings.TrimSuffix(entry.Name(), ".json")
				tasks[taskId] = task
				for _, queryFile := range task.QueryFiles {
					if _, err := os.Stat(queryFile); err != nil {
						log.Printf("%s links to an invalid query file %s\n", taskId, queryFile)
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
			task.Next = nil
		}
		for _, nextId := range task.NextIds {
			if nextId == taskId {
				log.Fatalf("cyclic reference in %s.next", taskId)
			}
			if nextIds[nextId] {
				log.Fatalf("%s.next got duplicated id %s", taskId, nextId)
			}
			if nextTask, exists := (*tm)[nextId]; exists {
				task.Next = append(task.Next, nextTask)
				nextTask.Prerequisites = append(nextTask.Prerequisites, task)
				nextIds[nextId] = true
			} else {
				log.Fatalf("%s.next %s is not found", taskId, nextId)
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
