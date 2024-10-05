package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
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
		// fmt.Println("map task:", reply.TaskId, "filename", reply.FileName)
		var intermediate []KeyValue

		file, err := os.Open(reply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.FileName)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
		}
		err = file.Close()

		kva := mapf(reply.FileName, string(content))
		intermediate = append(intermediate, kva...)

		buckets := make([][]KeyValue, reply.NReduce)
		for i := range buckets {
			buckets[i] = []KeyValue{}
		}
		for _, kv := range intermediate {
			buckets[ihash(kv.Key)%reply.NReduce] = append(buckets[ihash(kv.Key)%reply.NReduce], kv)
		}

		for i := range buckets {
			iName := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
			iFile, _ := os.CreateTemp("", iName+"*")
			enc := json.NewEncoder(iFile)
			for _, kv := range buckets[i] {
				err = enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write into %v", iName)
				}
			}
			err = os.Rename(iFile.Name(), iName)
			err = iFile.Close()
		}

		status = WorkerCompleted
	}

	runReduceTask := func(reply *Pong) {
		// fmt.Println("reduce task:", reply.TaskId, "filename", reply.FileName)
		var intermediate []KeyValue
		for i := 0; i < reply.NMap; i++ {
			iName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)

			file, err := os.Open(iName)
			if err != nil {
				log.Fatalf("cannot open %v %s", file, iName)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err = dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			err = file.Close()
		}

		sort.Slice(intermediate, func(i, j int) bool {
			return intermediate[i].Key < intermediate[j].Key
		})

		oName := "mr-out-" + strconv.Itoa(reply.TaskId)
		oFile, _ := os.CreateTemp("", oName+"*")

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			_, _ = fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		_ = os.Rename(oFile.Name(), oName)
		oFile.Close()

		for i = 0; i < reply.NMap; i++ {
			iName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskId)
			err := os.Remove(iName)
			if err != nil {
				log.Fatalf("cannot open delete" + iName)
			}
		}

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
			time.Sleep(100 * time.Millisecond)
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
			time.Sleep(100 * time.Millisecond)
		case jobFinish:
			// fmt.Println("jobFinish")
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
