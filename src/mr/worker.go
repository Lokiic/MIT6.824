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

	"../lab"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, id int) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	workerId := id
	alive := true
	attempt := 0

	for alive {
		attempt++
		fmt.Println(workerId, " worker ask", attempt)

		task := RequireTask(workerId)
		fmt.Println(workerId, "worker get task", task)
		switch task.TaskType {
		case lab.MapTask:
			{
				// 执行map任务
				DoMapTask(mapf, task)
				fmt.Println("do map task:", task.TaskId)
				// 上报任务完成
				FinishTask(workerId, task)
			}
		case lab.ReduceTask:
			{
				DoReduceTask(reducef, task)
				fmt.Println("do reduce task:", task.TaskId)
				FinishTask(workerId, task)
			}
		case lab.WaitingTask:
			{
				fmt.Println("get waiting task")
				time.Sleep(time.Second)
			}
		case lab.KillTask:
			{
				// 当接到kill任务时，当前worker退出
				time.Sleep(time.Second)
				fmt.Println("[Status]:", workerId, "terminated!!!")
				alive = false
			}
		}
		time.Sleep(time.Second)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallRpc() {

// 	// declare an argument structure.
// 	// args := ExampleArgs{}
// 	args := RpcArgs{}

// 	// fill in the argument(s).
// 	// args.X = 99

// 	// declare a reply structure.
// 	// reply := ExampleReply{}
// 	reply := RpcReply{}

// 	// send the RPC request, wait for the reply.
// 	// 发起RPC调用
// 	//call("Master.Example", &args, &reply)
// 	call("Master.RpcHandler", &args, &reply)

// 	// reply.Y should be 100.
// 	// fmt.Printf("reply.Y %v\n", reply.Y)
// 	fmt.Printf("worker get task from master, %s", reply.InputFile[0])
// }

// worker向master请求任务
func RequireTask(workerId int) *lab.Task {
	args := RpcArgs{}
	reply := lab.Task{}

	call("Master.DistributeTask", &args, &reply)
	fmt.Println("get master response", &reply)

	return &reply
}

// worker完成任务后向master上报
func FinishTask(workerId int, task *lab.Task) {
	args := task
	reply := RpcReply{}
	call("Master.TaskIsDone", &args, &reply)
}

// worker执行map任务
// 参考mrsequential.go
func DoMapTask(mapf func(string, string) []KeyValue, reply *lab.Task) {
	var intermediate []KeyValue
	fileName := reply.InputFile[0]

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannnot read %v", fileName)
	}
	file.Close()
	intermediate = mapf(fileName, string(content))

	rn := reply.ReduceNum // nReduce，即reduce任务数
	// 创建nReduce大小的数组，数组中存放的元素为[]KeyValue，通过ihash将每个Key放到对应的下标中
	// hashedKV := make([][]KeyValue, rn)

	// for _, kv := range intermediate {
	// 	hashedKV[ihash(kv.Key)%rn] = append(hashedKV[ihash(kv.Key)%rn], kv)
	// }

	// // 将中间kv对存储在对应的文件中，方便reduce任务读取
	// for i := 0; i < rn; i++ {
	// 	oname := "mr-tmp-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
	// 	ofile, _ := os.Create(oname)
	// 	enc := json.NewEncoder(ofile)
	// 	for _, kv := range hashedKV[i] {
	// 		enc.Encode(kv)
	// 	}
	// 	ofile.Close()
	// }

	// 使用临时文件
	// 如果进程由于某些原因退出了，这样产生的文件是不完整的，但reduce任务分不清哪些是成功的map输出，哪些不是
	dir, _ := os.Getwd()
	tmpFiles := make([]*os.File, rn)
	fileEncs := make([]*json.Encoder, rn)
	for i := 0; i < rn; i++ {
		tmpFiles[i], _ = ioutil.TempFile(dir, "mr-tmp-*")
		fileEncs[i] = json.NewEncoder(tmpFiles[i])
	}

	// 将中间kv对存储在对应的文件中，方便reduce任务读取
	for _, kv := range intermediate {
		index := ihash(kv.Key) % rn
		enc := fileEncs[index]
		enc.Encode(kv)
	}

	// 对临时文件重命名
	for index, file := range tmpFiles {
		oname := "mr-tmp-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(index)
		os.Rename(file.Name(), oname)
		file.Close()
	}
}

// reduce任务中用于读取中间文件
func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filePath := range files {
		file, _ := os.Open(filePath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

// worker 执行reduce任务
// 在reduce任务中执行sort，因为中间文件被分为很多份，经过reduce读取，将它们整合成一份，然后才能sort
func DoReduceTask(reducef func(string, []string) string, reply *lab.Task) {
	// 测试脚本希望在名为`mr-out-X`的文件中看到输出，每个reduce任务对应一个
	reduceFileNum := reply.TaskId
	intermediate := readFromLocalFile(reply.InputFile)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	// func TempFile(dir, pattern string) (f *os.File, err error)
	// TempFile在目录dir中创建一个新的临时文件，打开该文件进行读写，并返回生成的*os.File。文件名是通过接受pattern并在末尾添加一个随机字符串生成的。如果pattern包含"*"，则随机字符串替换最后一个"*"
	tmpFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmpFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	// 对临时文件重命名
	os.Rename(tmpFile.Name(), oname)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// 向master发送rpc请求，等待响应
// 返回true说明任务完成
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close() // 延迟关闭

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
