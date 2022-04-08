package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady	= 0
	TaskStatusQueue	= 1
	TaskStatusRunning = 2
	TaskStatusFinish 	= 3
	TaskStatusErr 	= 4

)

const (
	maxTaskRuntime = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)


type TaskStatus struct {
	
	Status	int
	WorkerId	int
	StartTime 	time.Time
}

type Coordinator struct {
	// Your definitions here.
	
	inputFiles 		[]string
	nReduce		int
	taskPhase		TaskPhase
	taskStats		[]TaskStatus
	mu 			sync.Mutex
	done 			bool
	workerSequence 	int
	taskChannel		chan	Task
}


func (c *Coordinator) getTask (taskSeq int) Task {
	
	task := Task {
		FileName:  "",
		NReduce: c.nReduce,
		NMaps: len(c.inputFiles),
		Seq: taskSeq,
		Phase: c.taskPhase,
		Alive: true,
	}
	if task.Phase == MapPhase {
		task.FileName = c.inputFiles[taskSeq]
	}
	return task
}


func (c *Coordinator) Schedule () {
	
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}

	allFinish := true
	
	for index, t := range c.taskStats {
		switch t.Status {

			case TaskStatusReady : 
				allFinish = false
				c.taskChannel <- c.getTask(index)
				c.taskStats[index].Status = TaskStatusQueue
			case TaskStatusQueue :
				allFinish = false
			case TaskStatusRunning :
				allFinish =  false
				if time.Now().Sub(t.StartTime) > maxTaskRuntime {
					c.taskChannel <- c.getTask(index)
					c.taskStats[index].Status = TaskStatusQueue
				}
			case TaskStatusFinish :
			case TaskStatusErr : 
				allFinish = false
				c.taskChannel <- c.getTask(index)
				c.taskStats[index].Status = TaskStatusQueue
			default :
				panic("t.Status Error")
		}
		if allFinish {
			if c.taskPhase == MapPhase {
				c.initReduceTasks()
			} else {
				c.done = true
			}
		}
	}
}

func (c *Coordinator) initMapTask (){
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStatus, len(c.inputFiles))
}

func (c *Coordinator) initReduceTasks () {
	c.taskPhase = ReducePhase
	c.taskStats = make([]TaskStatus, c.nReduce)
}

func (c *Coordinator) regTask (args *TaskArgs, task *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if task.Phase != c.taskPhase {
		panic("taskPhase Not Equal")
	}

	c.taskStats[task.Seq].Status = TaskStatusRunning
	c.taskStats[task.Seq].WorkerId = args.WorkerId
	c.taskStats[task.Seq].StartTime = time.Now()
}



func (c *Coordinator) GetOneTask (args *TaskArgs, reply *TaskReply) error {
	
	task := <-c.taskChannel
	reply.Task = &task

	if task.Alive {
		c.regTask(args, &task)
	}

	return nil
}


func (c *Coordinator) ReportTask (args *ReportTaskArgs, reply *ReportTaskReply) error {

	
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.taskPhase != args.Phase || args.WorkerId != c.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		c.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		c.taskStats[args.Seq].Status = TaskStatusErr
	}
	
	go c.Schedule()
	return nil
}

func (c *Coordinator) RegisterWorker (args *RegisterArgs, reply *RegisterReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.workerSequence += 1
	reply.WorkerId = c.workerSequence
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	return c.done
}

func (c *Coordinator) tickSchedule () {

	for !c.done	{
		go c.Schedule() 
		time.Sleep(ScheduleInterval)
	}
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mu = sync.Mutex{}
	c.nReduce = nReduce
	c.inputFiles = files

	if nReduce > len(files) {
		c.taskChannel = make (chan Task, nReduce)
	} else {
		c.taskChannel = make (chan Task, len(files))
	}

	c.initMapTask()

	go c.tickSchedule()
	c.server()


	return &c
}
