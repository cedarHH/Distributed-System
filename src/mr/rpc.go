package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	NoTask TaskType = iota
	MapTask
	ReduceTask
)

type WorkerStatus int

const (
	WorkerIdle WorkerStatus = iota
	WorkerBusy
	WorkerCompleted
	WorkerFailed
)

type Ping struct {
	Status   WorkerStatus `json:"Status"`
	TaskType TaskType     `json:"TaskType"`
	WorkerId int64        `json:"WorkerId"`
	TaskId   int          `json:"TaskId"`
	FileName string       `json:"FileName"`
}

type WorkerCommand int

const (
	waiting WorkerCommand = iota
	runTask
	inProgress
	jobFinish
)

type Pong struct {
	Command  WorkerCommand `json:"Command"`
	WorkerId int64         `json:"WorkerId"`
	TaskType TaskType      `json:"TaskType"`
	TaskId   int           `json:"TaskId"`
	FileName string        `json:"FileName"`
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
