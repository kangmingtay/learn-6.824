package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	isMapFinished := false
	for isMapFinished != true {
		resp := CallAssignMapTask()
		maptask := resp.Task
		nReduce := resp.NReduce

		if maptask.TaskNum != -1 {
			file, err := os.Open(maptask.Filename)
			defer file.Close()

			log.Printf("[Worker %v] Starting on map task: %+v\n", os.Getpid(), maptask.Filename)
			if err != nil {
				log.Fatalf("cannot open map file %v\n", err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v\n", maptask.Filename)
			}
			maptask.Result = mapf(maptask.Filename, string(content))

			intermediate := make(map[int][]KeyValue)
			for _, kv := range maptask.Result {
				reduceTaskNum := ihash(kv.Key) % nReduce
				intermediate[reduceTaskNum] = append(intermediate[reduceTaskNum], kv)
			}

			for i := 0; i < nReduce; i++ {
				tmpFileName := "tmp-" + strconv.Itoa(maptask.TaskNum) + "-" + strconv.Itoa(i) + ".txt"
				ifile, err := ioutil.TempFile("", tmpFileName)
				if err != nil {
					log.Fatalf("Cannot create ifile: %v\n", err)
				}

				enc := json.NewEncoder(ifile)
				for _, kv := range intermediate[i] {
					if err := enc.Encode(&kv); err != nil {
						log.Fatalf("Cannot write to file: %v\n", err)
					}
				}
				os.Rename(ifile.Name(), tmpFileName)
			}
		} else {
			log.Printf("[Worker %v] Waiting for other workers to finish...\n", os.Getpid())
			time.Sleep(1 * time.Second)
		}

		isMapFinished = CallCompleteMapTask(maptask)
	}

	isReduceFinished := false
	for isReduceFinished != true {
		reducetask := CallAssignReduceTask()

		if reducetask.TaskNum != -1 {
			log.Printf("[Worker %v] Starting on reduce task: %+v\n", os.Getpid(), reducetask)
			pattern := fmt.Sprintf("./tmp-*-%v.txt", reducetask.TaskNum)
			filenames, _ := filepath.Glob(pattern)
			var intermediate []KeyValue
			for _, p := range filenames {
				file, err := os.Open(p)
				defer file.Close()
				if err != nil {
					log.Fatalf("cannot open reduce %v\n", p)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			oname := "./mr-out-" + strconv.Itoa(reducetask.TaskNum)
			ofile, _ := os.Create(oname)
			defer ofile.Close()
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}

				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
		} else {
			log.Printf("[Worker %v] Waiting for other workers to finish...\n", os.Getpid())
			time.Sleep(1 * time.Second)
		}

		isReduceFinished = CallCompleteReduceTask(reducetask)
	}

}

//
// Make an RPC call to master to ask for an idle file
//
func CallAssignMapTask() MapTaskData {
	reply := MapTaskData{}

	if ok := call("Master.AssignMapTask", "", &reply); !ok {
		log.Fatalln("Error running map task")
	}
	return reply
}

//
// Tell master that current map task is completed
//
func CallCompleteMapTask(task MapTask) bool {
	var reply bool
	if ok := call("Master.CompleteMapTask", &task, &reply); !ok {
		log.Fatalln("Error running map task")
	}
	return reply
}

//
// Make an RPC call to master to ask for a reduce task
//
func CallAssignReduceTask() ReduceTask {
	reply := ReduceTask{}
	if ok := call("Master.AssignReduceTask", "", &reply); !ok {
		log.Fatalln("Error running reduce task")
	}
	return reply
}

//
// Make an RPC call to master to ask for an idle file
//
func CallCompleteReduceTask(task ReduceTask) bool {
	var reply bool
	if ok := call("Master.CompleteReduceTask", &task, &reply); !ok {
		log.Fatalln("Error running reduce task")
	}
	return reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
