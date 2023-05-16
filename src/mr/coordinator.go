package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	// 要处理的文件列表
	filenames []string
	// 处理状态 0-未处理 1-处理完成
	status map[string]int
	//reduce数量
	nReduce int
	//是否所有的文件都被map完成
	mapStatus bool
	// worker 和filename 映射
	mapWorkFile map[string]string
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

func (c *Coordinator) allocateFile(args *AllocateFileArgs, reply *AllocateFileReply) error {
	lock.Lock() // 加锁
	reply.status = false
	for _, v := range c.filenames {
		if _, ok := c.status[v]; !ok {
			log.Printf("文件%v 被分配给了worker %v\n", v, args.workerName)
			c.status[v] = 1
			c.mapWorkFile[v] = args.workerName
			reply.fileName = v
			reply.status = true
		}
	}
	lock.Unlock() // 解锁
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.filenames = append(c.filenames, files...)
	c.status = make(map[string]int)
	c.nReduce = nReduce
	c.mapWorkFile = make(map[string]string)
	c.mapStatus = false

	coordinator = &c

	log.Println("成功创建Coordinator,并开启服务")
	c.server()
	return &c
}
