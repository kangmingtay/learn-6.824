package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	// CallExample()
	maptask := CallAssignTask()
	file, err := os.Open(maptask.Filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v\n", maptask.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v\n", maptask.Filename)
	}
	maptask.Result = mapf(maptask.Filename, string(content))

	nReduce := CallCompleteTask(maptask)

	// slice of file encoders where index is reduceTaskNum
	var encoders []*json.Encoder
	for i := 0; i < nReduce; i++ {
		tmpFileName := "tmp-" + strconv.Itoa(maptask.TaskNum) + "-" + strconv.Itoa(i) + ".txt"
		// set file mode to allow RW operations
		tmpFile, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			log.Fatalf("Cannot open/create file: %v\n", err)
		}
		defer tmpFile.Close()
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	for _, kv := range maptask.Result {
		reduceTaskNum := ihash(kv.Key) % nReduce
		enc := encoders[reduceTaskNum]
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("Cannot write to file: %v\n", err)
		}
	}
}

//
// Make an RPC call to master to ask for an idle file
//
func CallAssignTask() MapTask {
	reply := MapTask{}

	if ok := call("Master.AssignTask", "", &reply); !ok {
		log.Fatalln("Error running map task")
	}
	fmt.Println(reply.Result)
	return reply
}

//
// Make an RPC call to master to ask for an idle file
//
func CallCompleteTask(task MapTask) int {
	var reply int
	if ok := call("Master.CompleteTask", &task, &reply); !ok {
		log.Fatalln("Error running map task")
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
