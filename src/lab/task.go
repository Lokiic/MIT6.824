package lab

import "time"

//import "time"

// 任务队列库
// type task struct {
// 	startTime time.Time
// 	fileName  string
// 	fileIndex int
// 	partIndex int
// 	nReduce   int
// 	nFiles    int
// }

type taskType int

const (
	MapTask     = 1
	ReduceTask  = 2
	WaitingTask = 3
	KillTask    = 4 // 所有任务完成后，再申请任务，得到的任务类型为kill
)

type Task struct {
	TaskType  taskType // 用于区分map任务和reduce任务以及等待任务
	InputFile []string // 输入文件数组，map任务中只有一个输入文件，reduce任务中有多个中间输入文件
	TaskId    int      // 任务id
	ReduceNum int      // reduce任务数
	// 通过taskId和reduceId可以生成中间结果文件 mr-tmp-taskId-reduceId
}

type taskCondition int

const (
	Waiting = 1
	Running = 2
	Done    = 3
)

// 任务元数据
type TaskMetaInfo struct {
	Condition taskCondition
	StartTime time.Time
	TaskPtr   *Task
}

// type mapTask struct {
// 	mt task
// }

// type reduceTask struct {
// 	rt task
// }

// type taskQueue struct {
// 	taskChan chan task
// }

// type taskQueueInterface interface {
// 	worker()
// }

// func (tq *taskQueue) worker() {
// 	for t := range tq.taskChan {
// 		process(t)
// 	}
// }
