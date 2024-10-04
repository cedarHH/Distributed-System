package mr

import (
	"fmt"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var (
		status   = WorkerIdle
		taskType = NoTask
		workerId int64
		taskId   int
		fileName string
		request  *Ping
	)

	runMapTask := func(reply *Pong) {
		fmt.Println("map task:", reply.TaskId, "filename", reply.FileName)
		time.Sleep(time.Second)
		status = WorkerCompleted
	}

	runReduceTask := func(reply *Pong) {
		fmt.Println("reduce task:", reply.TaskId, "filename", reply.FileName)
		time.Sleep(time.Second)
		status = WorkerCompleted
	}

	for {
		request = &Ping{
			Status:   status,
			TaskType: taskType,
			WorkerId: workerId,
			TaskId:   taskId,
			FileName: fileName,
		}
		reply := Pong{}
		call("Master.PingPong", request, &reply)
		switch reply.Command {
		case waiting:
			time.Sleep(time.Second)
			status = WorkerIdle
			workerId = reply.WorkerId
			taskId = reply.TaskId
			fileName = reply.FileName
		case runTask:
			status = WorkerBusy
			taskType = reply.TaskType
			workerId = reply.WorkerId
			taskId = reply.TaskId
			fileName = reply.FileName
			if reply.TaskType == MapTask {
				go runMapTask(&reply)
			} else if reply.TaskType == ReduceTask {
				go runReduceTask(&reply)
			}
		case inProgress:
			time.Sleep(time.Second)
		case jobFinish:
			fmt.Println("jobFinish")
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
