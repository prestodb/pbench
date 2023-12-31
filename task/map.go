package task

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"presto-benchmark/log"
)

type Map map[string]*Task

func (tm *Map) Get(taskId string) *Task {
	if tm == nil {
		return nil
	}
	return (*tm)[taskId]
}

const DefaultTaskFileExt = ".json"

func ParseTaskChain(startingFile string) (*Task, Map, error) {
	tasks := make(Map)
	startingTask, err := ParseTask(startingFile, tasks)
	if err == nil {
		err = checkTaskLinks(startingTask)
	}
	if err != nil {
		return nil, nil, err
	}
	return startingTask, tasks, nil
}

func ParseTask(filePath string, tasks Map) (*Task, error) {
	taskId := taskIdFromFilePath(filePath)
	task, ok := tasks[taskId]
	if ok {
		log.Debug().Msgf("%s already parsed, returned", taskId)
		return task, nil
	}
	log.Debug().Msgf("parsing task %s", taskId)
	task = &Task{
		Id: taskId,
	}
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
	}
	if err = json.Unmarshal(bytes, task); err != nil {
		return nil, fmt.Errorf("failed to parse json %s: %w", filePath, err)
	}
	dirPath := filepath.Dir(filePath)
	for i, queryFile := range task.QueryFiles {
		if !filepath.IsAbs(queryFile) {
			queryFile = filepath.Join(dirPath, queryFile)
			task.QueryFiles[i] = queryFile
		}
		if _, err = os.Stat(queryFile); err != nil {
			return nil, fmt.Errorf("%s links to an invalid query file %s: %w", taskId, queryFile, err)
		}
	}
	for i, nextTaskPath := range task.NextTaskPaths {
		if !filepath.IsAbs(nextTaskPath) {
			nextTaskPath = filepath.Join(dirPath, nextTaskPath)
			task.NextTaskPaths[i] = nextTaskPath
		}
		if _, err = os.Stat(nextTaskPath); err != nil {
			return nil, fmt.Errorf("%s links to an invalid next task file %s: %w", taskId, nextTaskPath, err)
		}
	}
	tasks[taskId] = task
	for _, nextTaskPath := range task.NextTaskPaths {
		if nextTask, err := ParseTask(nextTaskPath, tasks); err != nil {
			return nil, err
		} else {
			task.NextTasks = append(task.NextTasks, nextTask)
			nextTask.Prerequisites = append(nextTask.Prerequisites, task)
			nextTask.wgPrerequisites.Add(1)
		}
	}
	return task, nil
}

func taskIdFromFilePath(filePath string) string {
	// The task ID is the file name without directory path and extension.
	// It will be filePath[lastPathSeparator+1 : lastDot], so we have the following default values.
	lastPathSeparator, lastDot := -1, len(filePath)
	for i := 0; i < len(filePath); i++ {
		switch filePath[i] {
		case os.PathSeparator:
			lastPathSeparator = i
		case '.':
			lastDot = i
		}
	}
	if lastDot <= lastPathSeparator+1 || lastPathSeparator+1 >= len(filePath) {
		return filePath
	}
	return filePath[lastPathSeparator+1 : lastDot]
}

func checkTaskLinks(task *Task) error {
	nextTaskMap := make(map[string]bool)
	for _, nextTask := range task.NextTasks {
		if nextTaskMap[nextTask.Id] {
			return fmt.Errorf("task %s got duplicated next tasks %s", task.Id, nextTask.Id)
		}
		nextTaskMap[nextTask.Id] = true
		foundMyselfInNextTask := false
		prerequisiteMap := make(map[string]bool)
		for _, prerequisite := range nextTask.Prerequisites {
			if prerequisite == task {
				foundMyselfInNextTask = true
			}
			if prerequisiteMap[prerequisite.Id] {
				return fmt.Errorf("task %s got duplicated prerequisite tasks %s", nextTask.Id, prerequisite.Id)
			}
			prerequisiteMap[prerequisite.Id] = true
		}
		if !foundMyselfInNextTask {
			return fmt.Errorf("task %s not found in its next task %s's prerequisites", task.Id, nextTask.Id)
		}
		if err := checkTaskLinks(nextTask); err != nil {
			return err
		}
	}
	return nil
}
