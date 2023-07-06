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
	TaskChannelReduce chan *Task     // reduce task 队列，使用chan保证并发安全
	TaskChannelMap    chan *Task     // map task 队列，使用chan保证并发安全
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

// 发放任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	//	根据任务类型分配任务
	switch c.DistPhase {
	case MapPhase:
		{
			//如果map任务队列中还有任务时
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				//判断拿到的任务是否是等待状态
				if !c.TaskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting，其实是没这样的任务的
				if c.TaskMetaHolder.checkTaskDone() {
					c.toNextPhase()
					//更换任务阶段时，重置任务Id
					c.TaskId = 1
				}
				return nil
			}

		}
	default:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	lock.Lock()
	defer lock.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
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
		DistPhase:         MapPhase, //一开始初始化为map 阶段
		TaskChannelReduce: make(chan *Task, nReduce),
		TaskChannelMap:    make(chan *Task, len(files)),
		Files:             files,
		TaskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	//初始化map任务
	c.makeMapTasks(files)
	log.Println("成功创建Coordinator,并开启服务")
	c.server()
	return &c
}

// 对map任务进行处理,初始化map任务
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		task := Task{
			TaskId:   c.generateTaskId(),
			TaskType: MapTask,
			NReduce:  c.ReducerNum,
			FileName: v,
		}
		//任务加入Map任务管道
		c.TaskChannelMap <- &task
		//任务元信息加入map中
		taskMetaInfo := TaskMetaInfo{state: Waiting, TaskAddr: &task}
		c.TaskMetaHolder.acceptMetaInfo(&taskMetaInfo)
		fmt.Println("make a map task :", &task)
	}
}

func (c *Coordinator) makeReduceTasks(nReduce int) {
	for i := 1; i <= nReduce; i++ {
		task := Task{
			TaskId:   c.generateTaskId(),
			TaskType: ReduceTask,
		}
		c.TaskChannelReduce <- &task
		info := TaskMetaInfo{state: Waiting, TaskAddr: &task}
		c.TaskMetaHolder.acceptMetaInfo(&info)
		fmt.Println("make a reduce task", &task)
	}
}

func (c *Coordinator) generateTaskId() int {
	taskID := c.TaskId
	c.TaskId++
	return taskID
}

func (t *TaskMetaHolder) acceptMetaInfo(info *TaskMetaInfo) bool {
	taskId := info.TaskAddr.TaskId
	_, ok := t.MetaMap[taskId]
	if ok {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = info
	}
	return true
}

// 判断给定任务是否处于等待状态：如果是修改为运作中，返回true;否则返回false.
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	info, ok := t.MetaMap[taskId]
	if !ok || info.state != Waiting {
		return false
	}
	t.MetaMap[taskId].state = Working
	return true
}

// 检查多少个任务做了包括（map、reduce）,判断任务是不是都完成了
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		if v.TaskAddr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAddr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		//map 任务都完成了
		return true
	} else {
		//reduce 任务都完成了
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

// 转换状态
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = ExitPhase
	}
}
