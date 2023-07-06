package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.一直向协调器发送获取任务请求的工作器
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	keepFlag := true
	for keepFlag {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{

				doMapTask(mapf, &task)
				callMarkFinished(&task)
			}
		case WaitingTask:
			{
				fmt.Println("worker接受到了等待任务, please wait...")
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				fmt.Printf("start run reduce task- %d \n", task.TaskId)
				doReduceTask(reducef, &task)
				callMarkFinished(&task)
			}
		case ExitTask:
			{
				fmt.Println("-------------------------------------------------work process exited--------------------------------------------------------------------------------")
				keepFlag = false
			}
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

// GetTask 获取任务（需要知道是Map任务，还是Reduce）
func GetTask() Task {

	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// 向协调者发送消息，更新任务状态
func callMarkFinished(task *Task) {
	ok := call("Coordinator.MarkFinished", task, task)
	if !ok {
		fmt.Printf("call failed!\n")
		fmt.Printf("更新任务状态失败")
	}
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

// 完成 一次 map  任务
func doMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// 定义中间输出
	intermediate := []KeyValue{}
	//打开w
	file, err := os.Open(task.FileName[0])
	if err != nil {
		log.Fatalf("%v", err)
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	values := mapf(task.FileName[0], string(content))
	intermediate = append(intermediate, values...)
	file.Close()
	//initialize and loop over []KeyValue
	rn := task.NReduce
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i+1)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

// 完成一次 Reduce 任务
func doReduceTask(reducef func(key string, values []string) string, task *Task) {
	// 定义中间输出,从文件中读取出来
	intermediate := []KeyValue{}
	// 根据正则表达式进行文件名筛选
	for _, fileName := range task.FileName {
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("fail open file - %s \n", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()

	}
	sort.Sort(ByKey(intermediate))

	//进行reduce操作
	// 定义文件名，创建文件
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//得到某个word 的count
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}
