package mapReduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
//
// Overloaded function to recieve  both mapf and reducef function arguments :
// (key, value) && (IntermediateKey, List(Values))

func StartWorkers(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := Worker{}
	worker.register(mapf, reducef)
	worker.run()
}

func (w *Worker) requestTask() (Task, bool) {
	args := TaskArgs{WorkerId: w.id}
	reply := TaskReply{QuitWorker: false}

	if ok := call("Coordinator.GetTaskForWorker", &args, &reply); !ok {
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "RequestTask()",
		}).Fatalf("Get task for worker #%d failed", w.id)
	}

	return *reply.Task, reply.QuitWorker
}

func (w *Worker) run() {
	for {
		task, quitWorker := w.requestTask()

		if quitWorker {
			log.WithFields(log.Fields{
				"actorType":  "worker",
				"methodName": "Shutdown()",
			}).Info("Shutdown flag recieved. Exiting worker #", w.id)

			return
		}

		w.doTask(task)
	}
}

func (w *Worker) doTask(t Task) {
	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)

	case ReducePhase:
		w.doReduceTask(t)

	default:
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "doTask()",
		}).Fatalf("Invalid TaskPhase :%v", t.Phase)
	}
}

func (w *Worker) doMapTask(task Task) {
	log.WithFields(log.Fields{
		"actorType":  "worker",
		"methodName": "doMapTask()",
	}).Infof("Worker #%d picked up Map task #%d", w.id, task.Index)

	contents, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "doMapTask()",
		}).Error("Cannot ReadFile during MapTask")

		w.reportTask(task, err)
		return
	}

	reduceBuckets := make([][]KeyValue, task.NReduce)

	kvs := w.mapf(task.FileName, string(contents))
	for _, kv := range kvs {
		bucketIndex := ihash(kv.Key) % task.NReduce
		reduceBuckets[bucketIndex] = append(reduceBuckets[bucketIndex], kv)
	}

	for idx, mappedKVs := range reduceBuckets {
		filename := reduceName(task.Index, idx)
		f, err := os.Create(filename)
		if err != nil {
			log.WithFields(log.Fields{
				"actorType":  "worker",
				"methodName": "doMapTask()",
			}).Error("Cannot Create Reduce File")

			w.reportTask(task, err)
			return
		}

		enc := json.NewEncoder(f)
		for _, kv := range mappedKVs {
			if err := enc.Encode(&kv); err != nil {
				log.WithFields(log.Fields{
					"actorType":  "worker",
					"methodName": "doMapTask()",
				}).Error("Cannot encode KV pair")

				w.reportTask(task, err)
			}
		}
		if err := f.Close(); err != nil {
			log.WithFields(log.Fields{
				"actorType":  "worker",
				"methodName": "doMapTask()",
			}).Error("Cannot close reduce file")
			w.reportTask(task, err)
		}
	}

	w.reportTask(task, nil)
}

func (w *Worker) doReduceTask(task Task) {
	log.WithFields(log.Fields{
		"actorType":  "worker",
		"methodName": "doReduceTask()",
	}).Infof("Worker #%d picked up Reduce task #%d", w.id, task.Index)

	maps := make(map[string][]string)
	for idx := 0; idx < task.NMaps; idx++ {
		filename := reduceName(idx, task.Index)
		file, err := os.Open(filename)

		if err != nil {
			w.reportTask(task, err)
			return
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(task.Index), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(task, err)
	}

	w.reportTask(task, nil)
}

func (w *Worker) reportTask(task Task, err error) {
	args := ReportTaskArgs{
		Index:    task.Index,
		Phase:    task.Phase,
		WorkerId: w.id,
	}
	reply := ReportTaskReply{}

	if err == nil {
		args.Done = true
	}

	if ok := call("Coordinator.UpdateTaskStatus", &args, &reply); !ok {
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "reportTask()",
		}).Fatal("Worker reportTask fail")
	}
}

func (w *Worker) register(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := &RegisterArgs{}
	reply := &RegisterReply{}

	if ok := call("Coordinator.RegisterWorker", args, reply); !ok {
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "register()",
		}).Fatal("Worker Registration failed")
	}

	w.id = reply.WorkerId
	w.mapf = mapf
	w.reducef = reducef

	log.WithFields(log.Fields{
		"actorType":  "worker",
		"methodName": "doReduceTask()",
	}).Infof("Worker #%d registration success", w.id)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")

	sockname := getSocketDir()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
