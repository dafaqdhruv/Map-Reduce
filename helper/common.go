package mapReduce

import (
	"fmt"
	"time"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMaps    int
	Index    int
	Phase    TaskPhase
	Finished bool
}

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	maxTaskRuntime   = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type TaskStatus struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

func translateTaskStatus(t int) string {
	switch t {
	case TaskStatusReady:
		return "Ready"
	case TaskStatusQueue:
		return "In Queue"
	case TaskStatusRunning:
		return "Running"
	case TaskStatusFinish:
		return "Finished"
	case TaskStatusErr:
		return "Error"
	default:
		return "Invalid Status"
	}
}

func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
