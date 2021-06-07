package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Master struct {
	// Your definitions here.
	mu       sync.Mutex
	MapState []MapTask // index represents map task number
	NReduce  int       // read-only
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AssignTask(_ *string, reply *MapTask) error {
	fmt.Println("Worker called RPC...")
	// Master's state is global so need to lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// get idle file
	for taskNum, task := range m.MapState {
		if task.Progress == "idle" {
			reply.TaskNum = taskNum
			reply.Filename = task.Filename
			m.MapState[taskNum].Progress = "in-progress"
			break
		}
	}

	m.printMapStatus()

	return nil
}

func (m *Master) CompleteTask(task *MapTask, reply *int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MapState[task.TaskNum].Progress = "completed"
	*reply = m.NReduce

	m.printMapStatus()

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// Prints status of map tasks
//
func (m *Master) printMapStatus() {
	fmt.Println("===== MAP TASK STATUS =====")
	for _, task := range m.MapState {
		fmt.Println(task.Filename, task.Progress)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	// Returns true when MapReduce job is completely finished
	// Delete intermediate files after finished

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		NReduce: nReduce,
	}

	for _, file := range files {
		m.MapState = append(m.MapState, MapTask{
			Filename: file,
			Progress: "idle",
		})
	}

	// Your code here.
	// Divide intermediate keys into buckets for nReduce reduce tasks

	m.server()
	return &m
}
