package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
//
// TaskType 对于下方枚举任务的父类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask //拿到这个任务则说明任务均分配完成，等待任务执行结束
	ExitTask    //拿到这个任务则说明所有任务均完成
)

// State 任务的状态类型
type State int

const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// 任务对象，包含任务ID 任务类型、reduce任务的数量、以及文件名
type Task struct {
	TaskId   int
	TaskType TaskType
	NReduce  int
	FileName string
}

// TaskArgs rpc应该传入的参数，可实际上应该什么都不用传,因为只是worker获取一个任务
type TaskArgs struct{}

// Phase 对于分配任务阶段的父类型
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	ExitPhase
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type AllocateFileArgs struct {
	workerName string
}
type AllocateFileReply struct {
	fileName string
	status   bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
