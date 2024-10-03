package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var counter int64 = 0

func generateIncrementalIntUUID() int64 {
	return atomic.AddInt64(&counter, 1)
}

type TaskStatus int

const (
	TaskIdle TaskStatus = iota
	TaskInProgress
	TaskCompleted
	TaskFailed
)

type WorkerEntry struct {
	ID       string       // Worker ID
	Status   WorkerStatus // Idle, Busy, or Failed
	TaskID   int          // Task ID
	LastSeen time.Time    // Last heartbeat time
}

type TaskEntry struct {
	TaskID   int        // Task ID
	WorkerID int64      // Worker ID of the task being performed
	TaskType TaskType   // Task type
	Status   TaskStatus // Idle, InProgress, Completed, Failed
}

type Master struct {
	// Your definitions here.
	nMap             int
	nReduce          int
	mapFinished      int
	reduceFinished   int
	files            []string
	workers          sync.Map       // Save all Worker information
	taskChannel      chan TaskEntry // Channels for Task Assignments
	taskCompleteChan chan TaskEntry // Task Completion Notification Channel
	heartbeatChan    chan string    // Worker heartbeat channel for worker status notification
}

// Your code here -- RPC handlers for the worker to call.

func NewMaster(files []string, nReduce int) *Master {
	m := Master{
		nMap:             len(files),
		nReduce:          nReduce,
		files:            files,
		taskChannel:      make(chan TaskEntry),
		taskCompleteChan: make(chan TaskEntry),
		heartbeatChan:    make(chan string),
	}
	go m.taskScheduler()
	return &m
}

func (m *Master) taskScheduler() {
	go func() {
		for fileIdx := range m.files {
			m.taskChannel <- TaskEntry{
				TaskID:   fileIdx,
				TaskType: MapTask,
				Status:   TaskIdle,
			}
		}
	}()
	for task := range m.taskCompleteChan {
		if task.Status == TaskCompleted && task.TaskType == MapTask {
			m.mapFinished++
		}
		if m.mapFinished == m.nMap {
			break
		}
	}
	go func() {
		for reduceNum := range m.nReduce {
			m.taskChannel <- TaskEntry{
				TaskID:   reduceNum,
				TaskType: ReduceTask,
				Status:   TaskIdle,
			}
		}
	}()
	for task := range m.taskCompleteChan {
		if task.Status == TaskCompleted && task.TaskType == ReduceTask {
			m.reduceFinished++
		}
		if m.reduceFinished == m.nReduce {
			break
		}
	}
	close(m.taskChannel)
	close(m.taskCompleteChan)
}

func (m *Master) PingPong(args *Ping, reply *Pong) error {
	switch args.Status {
	case WorkerIdle: //idle
		m.handleIdleWorker(args, reply)
	case WorkerBusy: //working
		m.handleHeartbeat(args, reply)
	case WorkerFailed: // fatal
		m.handleFatal(args, reply)
	}
	return nil
}

func (m *Master) handleIdleWorker(args *Ping, reply *Pong) {
	assignTask := func(task TaskEntry) {
		if args.WorkerId == 0 {
			reply.WorkerId = generateIncrementalIntUUID()
		}
		reply.Command = runTask
		reply.TaskId = task.TaskID
		reply.TaskType = task.TaskType
		if task.TaskType == MapTask {
			reply.FileName = m.files[task.TaskID]
		}
	}

	if m.mapFinished < m.nMap || m.reduceFinished < m.nReduce {
		select {
		case task := <-m.taskChannel:
			assignTask(task)
		default:
			reply.Command = waiting
		}
	} else {
		reply.Command = jobFinish
	}
}

func (m *Master) handleHeartbeat(args *Ping, reply *Pong) {

}

func (m *Master) handleFatal(args *Ping, reply *Pong) {

}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := NewMaster(files, nReduce)
	go m.server()
	return m
}
