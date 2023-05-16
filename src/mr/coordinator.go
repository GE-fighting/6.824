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

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task     // 使用chan保证并发安全
	TaskChannelMap    chan *Task     // 使用chan保证并发安全
	TaskMetaHolder    TaskMetaHolder // 存着task
	Files             []string       // 传入的文件数组
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

type TaskMetaInfo struct {
	state    State
	TaskAddr *Task
}

var (
	coordinator *Coordinator //创建协调者
	lock        sync.Mutex   // 创建互斥锁
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//发放任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	//	根据任务类型分配任务
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.TaskMetaHolder.judgeState(reply.taskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.taskId)
				}
			} else {
				reply.taskType = WaitingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting，其实是没这样的任务的
				if c.TaskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}

		}
	default:
		{
			reply.taskType = ExitTask
		}
	}
	return nil
}
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	switch args.taskType {
	case MapTask:
		meta, ok := c.TaskMetaHolder.MetaMap[args.taskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.taskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.taskId)
		}
		break
	default:
		panic("The task type undefined ! ! !")
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	lock.Lock()
	defer lock.Unlock()
	if c.DistPhase == ExitPhase {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReducerNum:        nReduce,
		TaskId:            1,
		DistPhase:         MapPhase,
		TaskChannelReduce: make(chan *Task, nReduce),
		TaskChannelMap:    make(chan *Task, len(files)),
		Files:             files,
		TaskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	log.Println("成功创建Coordinator,并开启服务")
	c.server()
	return &c
}

// 对map任务进行处理,初始化map任务
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		task := Task{
			taskId:   c.generateTaskId(),
			taskType: MapTask,
			nReduce:  c.ReducerNum,
			fileName: v,
		}
		//任务加入Map任务管道
		c.TaskChannelMap <- &task
		//任务元信息加入map中
		taskMetaInfo := TaskMetaInfo{state: Waiting, TaskAddr: &task}
		c.TaskMetaHolder.acceptMetaInfo(&taskMetaInfo)
		fmt.Println("make a map task :", &task)
	}
}

func (c *Coordinator) generateTaskId() int {
	taskID := c.TaskId
	c.TaskId++
	return taskID
}

func (t *TaskMetaHolder) acceptMetaInfo(info *TaskMetaInfo) bool {
	taskId := info.TaskAddr.taskId
	_, ok := t.MetaMap[taskId]
	if ok {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = info
	}
	return true
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	info, ok := t.MetaMap[taskId]
	if !ok || info.state != Waiting {
		return false
	}
	t.MetaMap[taskId].state = Working
	return true
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		if v.TaskAddr.taskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAddr.taskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return true
}

//转换状态
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = ExitPhase
	}
}
