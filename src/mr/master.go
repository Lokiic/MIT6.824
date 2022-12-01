package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"../lab"
)

// master作为rpc的服务端，监听worker的rpc请求，并进行处理

type metaManager struct {
	MetaMap map[int]*lab.TaskMetaInfo
}

type masterCondition int

const (
	MapPhase    = 1
	ReducePhase = 2
	Done        = 3
)

type Master struct {
	// Your definitions here.
	// 通过两个channel来存放map任务和reduce任务
	taskMap         chan *lab.Task
	taskReduce      chan *lab.Task
	reduceNum       int
	mapNum          int
	unqiueTaskId    int // 自增的taskId，保证每个任务的id不同
	Condition       masterCondition
	taskMetaManager metaManager // 元数据管理相关
	mu              sync.Mutex
}

// 放入任务
func (m *metaManager) putTaskMetaInfo(info *lab.TaskMetaInfo) bool {
	taskId := info.TaskPtr.TaskId
	meta, _ := m.MetaMap[taskId]
	if meta != nil {
		fmt.Printf("taskId = %v is exist", taskId)
		return false
	} else {
		m.MetaMap[taskId] = info
	}
	return true
}

// 根据任务id拿到任务元数据信息
func (m *metaManager) getTaskMetaInfo(tId int) (bool, *lab.TaskMetaInfo) {
	info, ok := m.MetaMap[tId]
	if ok {
		return true, info
	}
	return false, nil
}

// 任务状态变更，元数据管理器对任务状态和运行时间进行变更
func (m *metaManager) changeTaskCondition(tId int) bool {
	ok, taskInfo := m.getTaskMetaInfo(tId)
	if !ok || taskInfo.Condition != lab.Waiting {
		return false
	}
	taskInfo.Condition = lab.Running
	taskInfo.StartTime = time.Now()
	return true
}

// 检查当前阶段是否完成（统计reduce任务和map任务完成和未完成的数量）
// 只有当map任务全部完成或者reduce任务全部完成才能进入下一阶段
func (m *metaManager) checkTaskDone() bool {
	reduceDoneNum := 0
	reduceNotDoneNum := 0
	mapDoneNum := 0
	mapNotDoneNum := 0
	for _, info := range m.MetaMap {
		if info.TaskPtr.TaskType == lab.MapTask {
			if info.Condition == lab.Done {
				mapDoneNum++
			} else {
				mapNotDoneNum++
			}
		} else {
			if info.Condition == lab.Done {
				reduceDoneNum++
			} else {
				reduceNotDoneNum++
			}
		}
	}
	fmt.Printf("%d / %d map tasks are done, %d / %d reduce tasks are done", mapDoneNum, mapDoneNum+mapNotDoneNum, reduceDoneNum, reduceDoneNum+reduceNotDoneNum)
	// reduceDoneNum > 0说明已经处于reduce阶段，如果当前阶段结束，那么所有reduce任务都应该已经结束
	// map任务同理
	return (reduceDoneNum > 0 && reduceNotDoneNum == 0) || (mapDoneNum > 0 && mapNotDoneNum == 0)
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// Go RPC的函数必须是导出的（首字母大写）
// func (m *Master) RpcHandler(args *RpcArgs, reply *RpcReply) error {
// 	reply.Task = *<-m.taskMap
// 	return nil
// }

// master分发任务
func (m *Master) DistributeTask(args *RpcArgs, reply *lab.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println("master get a request from worker")
	if m.Condition == MapPhase {
		if len(m.taskMap) > 0 {
			*reply = *<-m.taskMap
			if !m.taskMetaManager.changeTaskCondition(reply.TaskId) {
				fmt.Printf("[duplicated task id!!!] task %d is not waiting", reply.TaskId)
			}
		} else {
			// 队列中没有map任务，此时需要检查当前阶段所有map任务是否已经完成
			reply.TaskType = lab.WaitingTask
			if m.taskMetaManager.checkTaskDone() {
				fmt.Println("\nMap Phase is finished!!!")
				m.changePhase()
			}
			return nil
		}
	} else if m.Condition == ReducePhase {
		if len(m.taskReduce) > 0 {
			*reply = *<-m.taskReduce
			if !m.taskMetaManager.changeTaskCondition(reply.TaskId) {
				fmt.Printf("[duplicated task id!!!] task %d is not waiting", reply.TaskId)
			}
		} else {
			// 队列中没有reduce任务，此时需要检查当前阶段所有reduce任务是否已经完成
			reply.TaskType = lab.WaitingTask
			if m.taskMetaManager.checkTaskDone() {
				fmt.Println("Reduce Phase is finished!!!")
				m.changePhase()
			}
			return nil
		}
	} else {
		// 所有任务已经完成，那么任务类型为kill
		reply.TaskType = lab.KillTask
	}
	return nil
}

// worker上报给master，表示任务已经完成，此时需要判断该任务是否是重复完成的
// 因为可能任务最开始交给worker1，但worker1由于故障等问题，一直没完成任务导致任务超时
// 此时任务会进入channel重新分发，当新worker完成任务之后，worker1也完成了任务，那么就会导致任务重复完成
func (m *Master) TaskIsDone(args *lab.Task, reply *RpcReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.TaskType {
	case lab.MapTask:
		ok, info := m.taskMetaManager.getTaskMetaInfo(args.TaskId)
		if ok && info.Condition == lab.Running {
			// 如果该task正在运行，此时worker上报master表示任务已经完成时，可以直接修改
			info.Condition = lab.Done
			fmt.Printf("Map task: %d complete\n", args.TaskId)
		} else {
			fmt.Printf("[duplicated!!!] task: %d done", args.TaskId)
		}
	case lab.ReduceTask:
		ok, info := m.taskMetaManager.getTaskMetaInfo(args.TaskId)
		if ok && info.Condition == lab.Running {
			// 如果该task正在运行，此时worker上报master表示任务已经完成时，可以直接修改
			info.Condition = lab.Done
			fmt.Printf("Reduce task: %d complete\n", args.TaskId)
		} else {
			fmt.Printf("[duplicated!!!] task: %d done", args.TaskId)
		}
	default:
		panic("wrong task done")
	}
	return nil
}

// master的状态转换
// 由于map任务和reduce任务存放在channel中，所以当channel长度为0时，可以去检查一下当前阶段任务是否全部完成
func (m *Master) changePhase() {
	if m.Condition == MapPhase {
		fmt.Println("\nMap phase -> Reduce phase")
		m.makeReduceTasks()
		m.Condition = ReducePhase
	} else if m.Condition == ReducePhase {
		m.Condition = Done
	}
}

// 生成任务Id
func (m *Master) generatedTaskId() int {
	// m.mu.Lock()
	// defer m.mu.Unlock()
	res := m.unqiueTaskId
	m.unqiueTaskId++
	return res
}

// 中间文件处理函数
func TmpFileHandler(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	// os.Getwd返回与当前目录对应的根路径名。如果可以通过多条路径到达当前目录(由于有符号链接)，则Getwd可能返回其中任何一条路径
	path, _ := os.Getwd()
	// func ReadDir(dirname string) ([]fs.FileInfo, error)
	// ReadDir读取以dirname命名的目录并返回一个fs列表。FileInfo为目录内容，按文件名排序
	/*
		type FileInfo interface {
			Name() string       // base name of the file
			Size() int64        // length in bytes for regular files; system-dependent for others
			Mode() FileMode     // file mode bits
			ModTime() time.Time // modification time
			IsDir() bool        // abbreviation for Mode().IsDir()
			Sys() any           // underlying data source (can return nil)
		}
	*/
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		// 判断文件名的前缀和后缀是否符合 ”mr-tmp-x-y“
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

// master生成reduce任务
func (m *Master) makeReduceTasks() {
	fmt.Println("\nstart making reduce job!")
	for i := 0; i < m.reduceNum; i++ {
		id := m.generatedTaskId()
		fmt.Println("making reduce job:", id)
		task := lab.Task{
			TaskType:  lab.ReduceTask,
			TaskId:    id,
			InputFile: TmpFileHandler(i, "main/mr-tmp"),
		}

		metaInfo := lab.TaskMetaInfo{
			Condition: lab.Waiting,
			TaskPtr:   &task,
		}

		m.taskMetaManager.putTaskMetaInfo(&metaInfo)
		m.taskReduce <- &task
	}
	fmt.Println("making reduce task done")
	//m.taskMetaManager.checkTaskDone()
}

// master生成map任务
func (m *Master) makeMapTasks(files []string) {
	for _, v := range files {
		id := m.generatedTaskId()
		fmt.Println("making map task:", id)
		task := lab.Task{
			TaskType:  1, // 1表示map任务，2表示reduce任务
			InputFile: []string{v},
			TaskId:    id,
			ReduceNum: m.reduceNum,
		}

		metaInfo := lab.TaskMetaInfo{
			Condition: lab.Waiting,
			TaskPtr:   &task,
		}

		m.taskMetaManager.putTaskMetaInfo(&metaInfo)
		fmt.Println("making map task:", &task)
		m.taskMap <- &task
	}
	fmt.Println("making map task done")
	//m.taskMetaManager.checkTaskDone()
}

// 超时任务处理
func (m *Master) TimeOutHandler() {
	for {
		// 每次检测超时任务之间，需要有一定的间隔
		time.Sleep(time.Second * 2)
		m.mu.Lock()
		if m.Condition == Done {
			m.mu.Unlock()
			break
		}

		timeNow := time.Now()
		for _, v := range m.taskMetaManager.MetaMap {
			fmt.Println(v)
			if v.Condition == lab.Running {
				fmt.Println("task:", v.TaskPtr.TaskId, "working for:", timeNow.Sub(v.StartTime)) // 打印任务已经执行了多长时间
			}
			if v.Condition == lab.Running && timeNow.Sub(v.StartTime) > 10*time.Second {
				// 超时处理
				fmt.Println("detect a timeout job:", v.TaskPtr.TaskId)
				switch v.TaskPtr.TaskType {
				case lab.MapTask:
					{
						m.taskMap <- v.TaskPtr
						v.Condition = lab.Waiting
					}
				case lab.ReduceTask:
					{
						m.taskReduce <- v.TaskPtr
						v.Condition = lab.Waiting
					}
				}
			}
		}
		m.mu.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
// 启动一个线程监听来自worker的rpc请求
//
func (m *Master) server() {
	rpc.Register(m)  // 注册rpc服务
	rpc.HandleHTTP() // 将rpc挂载到http上
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.Condition == Done {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		taskMap:    make(chan *lab.Task, len(files)), // 一个map任务来自于一个输入文件
		taskReduce: make(chan *lab.Task, nReduce),    // 有nReduce个worker进行reduce任务，最多可能有nReduce个reduce任务
		taskMetaManager: metaManager{
			// map用来存放任务元数据
			MetaMap: make(map[int]*lab.TaskMetaInfo, len(files)+nReduce),
		},
		Condition:    MapPhase, // 创建master时，处于map阶段
		reduceNum:    nReduce,
		mapNum:       len(files),
		unqiueTaskId: 0,
	}

	// Your code here.
	m.makeMapTasks(files)
	m.server()

	// 起一个goroutine进行超时处理
	go m.TimeOutHandler()
	return &m
}
