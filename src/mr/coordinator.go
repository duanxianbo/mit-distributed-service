package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MRState string

const (
	mapping  MRState = "mapping"
	reducing MRState = "reducing"
	finished MRState = "finished"
)

type Coordinator struct {
	nextWorkerId int
	nextTaskId   int
	mapTasks     map[int]*MapTask
	reduceTasks  map[int]*ReduceTask
	nReducer     int
	nMapper      int
	phrase       MRState
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {

	c.getNextIdleTask()

	return nil
}

func (c *Coordinator) getNextIdleTask() (int, error) {
	if c.phrase == mapping {
		if c.nextTaskId == len(c.mapTaskState) {
			return errors.New("no more")
		}
	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
