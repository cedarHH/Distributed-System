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
	ID       int64        // Worker ID
	Status   WorkerStatus // Idle, Busy, or Failed
	TaskID   int          // Task ID
	TaskType TaskType     // Task type
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
	done             chan struct{}
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
		done:             make(chan struct{}),
	}
	go m.taskScheduler()
	return &m
}

func (m *Master) taskScheduler() {
	go m.Daemon()
	go func() {
		for fileIdx := range m.files {
			m.taskChannel <- TaskEntry{
				TaskID:   fileIdx,
				TaskType: MapTask,
				Status:   TaskIdle,
			}
		}
	}()
	// fmt.Println("waiting map task completed")
	for task := range m.taskCompleteChan {
		if task.Status == TaskCompleted && task.TaskType == MapTask {
			// fmt.Println("Map Finished:", task.TaskID)
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
			// fmt.Println("Reduce Finished", task.TaskID)
			m.reduceFinished++
		}
		if m.reduceFinished == m.nReduce {
			break
		}
	}
	// fmt.Println("finished")
	// close(m.taskChannel)
	// close(m.taskCompleteChan)
	time.Sleep(200 * time.Millisecond)
	m.done <- struct{}{}
}

func (m *Master) Daemon() {
	for {
		m.workers.Range(func(key, value any) bool {
			entry, ok := value.(*WorkerEntry)
			if !ok {
				return true
			}

			if entry.Status == WorkerBusy && time.Since(entry.LastSeen) > 500*time.Millisecond {
				// fmt.Printf("crash, type:%v, id:%v\n", entry.TaskType, entry.TaskID)
				m.taskChannel <- TaskEntry{
					TaskID:   entry.TaskID,
					TaskType: entry.TaskType,
					Status:   TaskIdle,
				}
				m.workers.Delete(entry.ID)
			}
			return true
		})
		time.Sleep(500 * time.Millisecond)
	}
}

func (m *Master) PingPong(args *Ping, reply *Pong) error {
	switch args.Status {
	case WorkerIdle: //idle
		m.handleIdleWorker(args, reply)
	case WorkerBusy: //working
		m.handleHeartbeat(args, reply)
	case WorkerCompleted:
		m.handleCompletedWorker(args, reply)
	case WorkerFailed: // fatal
		m.handleFatal(args, reply)
	}
	if actual, exists := m.workers.Load(reply.WorkerId); exists == true {
		entry := actual.(*WorkerEntry)
		entry.Status = reply.Status
		entry.TaskID = reply.TaskId
		entry.TaskType = reply.TaskType
		entry.LastSeen = time.Now()
	}
	return nil
}

func (m *Master) handleIdleWorker(args *Ping, reply *Pong) {
	assignTask := func(task TaskEntry) {
		// fmt.Println("assign task:", task.TaskID)
		reply.Command = runTask
		reply.Status = WorkerBusy
		reply.TaskId = task.TaskID
		reply.TaskType = task.TaskType
		reply.NReduce = m.nReduce
		reply.NMap = m.nMap
		if task.TaskType == MapTask {
			reply.FileName = m.files[task.TaskID]
		}
	}

	if args.WorkerId == 0 {
		args.WorkerId = generateIncrementalIntUUID()
		m.workers.Store(args.WorkerId, &WorkerEntry{
			ID:       args.WorkerId,
			Status:   WorkerIdle,
			TaskID:   0,
			TaskType: NoTask,
			LastSeen: time.Now(),
		})
	}
	reply.WorkerId = args.WorkerId
	if m.mapFinished < m.nMap || m.reduceFinished < m.nReduce {
		select {
		case task := <-m.taskChannel:
			assignTask(task)
			// fmt.Println("assign task:", task.TaskID)
		default:
			reply.Command = waiting
			reply.Status = WorkerIdle
			// fmt.Println("waiting")
		}
	} else {
		reply.Command = jobFinish
		reply.Status = WorkerCompleted
	}
}

func (m *Master) handleHeartbeat(args *Ping, reply *Pong) {
	*reply = Pong{
		Command:  inProgress,
		Status:   WorkerBusy,
		WorkerId: args.WorkerId,
		TaskType: args.TaskType,
		TaskId:   args.TaskId,
		FileName: args.FileName,
	}
}

func (m *Master) handleCompletedWorker(args *Ping, reply *Pong) {
	go func(taskId int, workerId int64, taskType TaskType) {
		task := TaskEntry{
			TaskID:   taskId,
			WorkerID: workerId,
			TaskType: taskType,
			Status:   TaskCompleted,
		}
		m.taskCompleteChan <- task
	}(args.TaskId, args.WorkerId, args.TaskType)
	// fmt.Println("receive a completed task", args.TaskId)
	args.Status = WorkerIdle
	reply.WorkerId = args.WorkerId
	m.handleIdleWorker(args, reply)
}

func (m *Master) handleFatal(args *Ping, reply *Pong) {
	// todo
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
func (m *Master) Done() (ret bool) {
	// Your code here.
	select {
	case <-m.done:
		ret = true
	default:
		ret = false
	}
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
