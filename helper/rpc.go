package mapReduce

import (
	"os"
	"strconv"
)

type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Task       *Task
	QuitWorker bool
}

type ReportTaskArgs struct {
	Done     bool
	Index    int
	Phase    TaskPhase
	WorkerId int
}

type ReportTaskReply struct {
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func getSocketDir() string {
	// s := "/var/tmp/824-mr-"
	s, _ := os.Getwd()
	s += "/dat/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
