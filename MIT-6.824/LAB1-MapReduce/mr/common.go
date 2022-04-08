package mr

import (
	"fmt"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName      string
	NReduce       int
	NMaps         int
	Seq           int
	Phase         TaskPhase
	Alive         bool	// worker should exit when alive is false

}


func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

// since output of each reduce task as format mr-out-X
func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
