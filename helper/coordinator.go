package mapReduce

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Coordinator struct {
	inputFiles   []string
	nReduce      int
	nMap         int
	taskPhase    TaskPhase
	taskStats    []TaskStatus
	mu           sync.Mutex
	taskChannel  chan Task
	workersCount int

	jobsFinished bool // is this even required?
	// timeout factor?
}

// create a Coordinator.
// nReduce is the number of reduce jobs to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu = sync.Mutex{}
	c.nReduce = nReduce
	c.inputFiles = files
	c.nMap = len(c.inputFiles)

	// why should the channel length be the smaller one?

	if c.nReduce > c.nMap {
		// if c.nReduce < c.nMap {
		c.taskChannel = make(chan Task, c.nReduce)
	} else {
		c.taskChannel = make(chan Task, c.nMap)
	}

	c.initMapTask()

	go c.tickSchedule()

	c.server()
	return &c
}

func (c *Coordinator) initMapTask() {
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStatus, c.nMap)
}

func (c *Coordinator) initReduceTasks() {
	c.taskPhase = ReducePhase
	c.taskStats = make([]TaskStatus, c.nReduce)
}

func (c *Coordinator) getTask(taskIndex int) Task {
	task := Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMaps:    len(c.inputFiles),
		Index:    taskIndex,
		Phase:    c.taskPhase,
		Finished: c.taskStats[taskIndex].Status == TaskStatusFinish,
	}

	if task.Phase == MapPhase {
		task.FileName = c.inputFiles[taskIndex]
	}
	return task
}

// Checks job status and adds to channel queue
func (c *Coordinator) ScheduleJobs() error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.jobsFinished {
		log.WithFields(log.Fields{
			"actorType":  "coordinator",
			"methodName": "ScheduleJobs()",
		}).Info("All Jobs Finished! \nShutting down Coordinator \n")

		return nil
	}

	allJobsFinished := true
	for index, task := range c.taskStats {
		log.WithFields(log.Fields{
			"actorType":  "coordinator",
			"methodName": "ScheduleJobs()",
		}).Debug("Status for task #", index, "is ", translateTaskStatus(task.Status))

		switch task.Status {
		case TaskStatusReady:
			allJobsFinished = false

			c.taskChannel <- c.getTask(index)
			c.taskStats[index].Status = TaskStatusQueue

		case TaskStatusQueue:
			allJobsFinished = false

			// Resend if channel is empty and task is still queued
			// Assuming TaskStatus changes on every task read from channel
			// if len(c.taskChannel) == 0 {
			// 	c.taskChannel <- c.getTask(index)
			// }

		case TaskStatusRunning:
			allJobsFinished = false

			// Drop task if worker takes too long
			if time.Since(task.StartTime) > maxTaskRuntime {
				log.WithFields(log.Fields{
					"actorType":  "coordinator",
					"methodName": "ScheduleJobs()",
				}).Infof("Worker #%d took too long on Task #%d. Dropping", task.WorkerId, index)

				c.taskChannel <- c.getTask(index)
				c.taskStats[index].Status = TaskStatusQueue
			}

		case TaskStatusFinish:

		case TaskStatusErr:
			allJobsFinished = false
			c.taskChannel <- c.getTask(index)
			c.taskStats[index].Status = TaskStatusQueue

		default:
			log.WithFields(log.Fields{
				"actorType":  "coordinator",
				"methodName": "ScheduleJobs()",
			}).Fatal("task.Status Error: ", task.Status)
		}
	}

	if allJobsFinished {
		if c.taskPhase == MapPhase {
			c.initReduceTasks()
			c.taskChannel <- c.getTask(0)
		} else {
			c.jobsFinished = true
			log.WithFields(log.Fields{
				"actorType":  "coordinator",
				"methodName": "ScheduleJobs()",
			}).Info("All Jobs finished. Closing Task Channel")
			close(c.taskChannel)
		}
	}

	return nil
}

func (c *Coordinator) AssignTaskToWorker(task *Task, WorkerId int) {
	log.WithFields(log.Fields{
		"actorType":  "coordinator",
		"methodName": "AssignTaskToWorker()",
	}).Info("Assigning Task to Worker ", WorkerId, "to task ", task.Index)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.taskStats[task.Index] = TaskStatus{
		Status:    TaskStatusRunning,
		WorkerId:  WorkerId,
		StartTime: time.Now(),
	}
}

func (c *Coordinator) GetTaskForWorker(args *TaskArgs, reply *TaskReply) error {
	log.WithFields(log.Fields{
		"actorType":  "coordinator",
		"methodName": "GetTaskForWorker()",
	}).Tracef("Getting task for worker #%d. Length of TaskChannel: %d", args.WorkerId, len(c.taskChannel))

	task, channelOpen := <-c.taskChannel

	if !channelOpen {
		log.WithFields(log.Fields{
			"actorType":  "coordinator",
			"methodName": "GetTaskForWorker()",
		}).Info("Task channel closed. Quitting worker #", args.WorkerId)

		reply.Task = new(Task)
		reply.Task.Finished = true

		reply.QuitWorker = true
		return nil
	}

	reply.Task = &task

	if !reply.Task.Finished {
		c.AssignTaskToWorker(reply.Task, args.WorkerId)
	}

	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.taskPhase != args.Phase || args.WorkerId != c.taskStats[args.Index].WorkerId {
		log.WithFields(log.Fields{
			"actorType":  "coordinator",
			"methodName": "UpdateTaskStatus()",
		}).Fatal("Mismatched Task Args. ", c.taskPhase, args.Phase, args.WorkerId, c.taskStats[args.Index].WorkerId)
	}

	if args.Done {
		c.taskStats[args.Index].Status = TaskStatusFinish
	} else {
		c.taskStats[args.Index].Status = TaskStatusErr
	}

	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.workersCount += 1
	reply.WorkerId = c.workersCount

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// l, e := net.Listen("tcp", ":1234")

	rpc.Register(c)
	rpc.HandleHTTP()

	sockname := getSocketDir()
	os.Remove(sockname)
	os.MkdirAll(filepath.Dir(sockname), 0775)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.WithFields(log.Fields{
			"actorType":  "coordinator",
			"methodName": "UpdateTaskStatus()",
		}).Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls jobsFinished() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) JobsFinished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.jobsFinished
}

func (c *Coordinator) tickSchedule() {
	for !c.JobsFinished() {
		go c.ScheduleJobs()
		time.Sleep(ScheduleInterval)
	}
}
