package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}



type TaskArgs struct {
	WorkerId 	int
}

type TaskReply struct {
	Task *Task
}

type ReportTaskArgs struct {
	Done 	bool
	Seq 	int
	Phase	TaskPhase
	WorkerId int
}

type ReportTaskReply struct {
}

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId	int
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
