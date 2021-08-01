package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	MapState    []MapTask // index represents map task number
	ReduceState []ReduceTask
	NReduce     int // read-only
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AssignMapTask(_ *string, reply *MapTaskData) error {
	// Master's state is global so need to lock
	m.mu.Lock()
	defer m.mu.Unlock()

	var newtask MapTask
	// get idle file
	for i, task := range m.MapState {
		// need to handle case where worker crash but task is still "in-progress"
		// if task.Progress == "in-progress" {
		// 	currentTime := time.Now()
		// 	if duration := task.StartedAt.Add(time.Duration(10) * time.Second); currentTime.After(duration) {
		// 		// reset task progress
		// 		// log.Printf("[Master] Reassign task: %v\n", task.Filename)
		// 		// log.Printf("[Master] Timings: %v, %v, %v\n", task.StartedAt, duration, currentTime)
		// 		m.MapState[i].Progress = "idle"
		// 	}
		// }

		if task.Progress == "idle" {
			newtask.TaskNum = task.TaskNum
			newtask.Filename = task.Filename
			newtask.Progress = "in-progress"
			newtask.StartedAt = time.Now()
			m.MapState[i].StartedAt = newtask.StartedAt
			m.MapState[i].Progress = "in-progress"
			break
		}
	}

	if newtask.Filename == "" {
		// all tasks have been assigned already
		log.Println("[Master] All tasks are assigned.")
		newtask.TaskNum = -1
	}
	reply.Task = newtask
	reply.NReduce = m.NReduce

	// m.printMapStatus()

	return nil
}

func (m *Master) CompleteMapTask(task *MapTask, reply *bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// need to check if worker completed task on time
	// if worker did not, then do not set progress to completed

	if task.TaskNum != -1 {
		// worker was assigned an empty task
		m.MapState[task.TaskNum].Progress = "completed"
	}

	isMapFinished := true
	for _, task := range m.MapState {
		if task.Progress != "completed" {
			isMapFinished = false
			break
		}
	}
	*reply = isMapFinished

	// m.printMapStatus()

	return nil
}

func (m *Master) AssignReduceTask(_ *string, reply *ReduceTask) error {
	done := false
	for done != true {
		// waits till all map tasks are completed
		done = m.isMapDone()
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	var newtask ReduceTask
	for taskNum, task := range m.ReduceState {
		// currentTime := time.Now()
		// if task.Progress == "in-progress" {
		// 	if duration := task.StartedAt.Add(time.Duration(10) * time.Second); currentTime.After(duration) {
		// 		// reset task progress
		// 		m.ReduceState[taskNum].Progress = "idle"
		// 	}
		// }
		if task.Progress == "idle" {
			newtask.TaskNum = taskNum
			newtask.Progress = "in-progress"
			newtask.StartedAt = time.Now()
			m.ReduceState[taskNum].StartedAt = newtask.StartedAt
			m.ReduceState[taskNum].Progress = "in-progress"
			break
		}
	}

	if newtask.Progress == "" {
		newtask.TaskNum = -1
	}

	reply.TaskNum = newtask.TaskNum
	reply.Progress = newtask.Progress
	reply.StartedAt = newtask.StartedAt

	return nil
}

func (m *Master) CompleteReduceTask(task *ReduceTask, reply *bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// need to check if worker completed task on time
	// if worker did not, then do not set progress to completed

	if task.TaskNum != -1 {
		m.ReduceState[task.TaskNum].Progress = "completed"
	}

	isReduceFinished := true
	for _, task := range m.ReduceState {
		if task.Progress != "completed" {
			isReduceFinished = false
			break
		}
	}
	*reply = isReduceFinished

	// m.printReduceStatus()
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
// Prints status of reduce tasks
//
func (m *Master) printReduceStatus() {
	fmt.Println("===== REDUCE TASK STATUS =====")
	for _, task := range m.ReduceState {
		fmt.Println(task.TaskNum, task.Progress)
	}
}

func (m *Master) isMapDone() bool {
	for _, task := range m.MapState {
		if task.Progress != "completed" {
			return false
		}
	}
	return true
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
	ret := true

	// Your code here.
	// Returns true when MapReduce job is completely finished
	// Delete intermediate files after finished
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, task := range m.ReduceState {
		if task.Progress != "completed" {
			ret = false
			break
		}
	}

	if ret {
		pattern := "./tmp-*-*.txt"
		filenames, _ := filepath.Glob(pattern)
		for _, filename := range filenames {
			err := os.Remove(filename)
			if err != nil {
				log.Fatalf("cannot remove %v: %v\n", filename, err)
			}
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		NReduce: nReduce,
	}

	for taskNum, file := range files {
		m.MapState = append(m.MapState, MapTask{
			TaskNum:  taskNum,
			Filename: file,
			Progress: "idle",
		})
	}

	for i := 0; i < nReduce; i++ {
		m.ReduceState = append(m.ReduceState, ReduceTask{
			TaskNum:  i,
			Progress: "idle",
		})
	}

	m.server()
	return &m
}
