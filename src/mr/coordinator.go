package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义为全局，worker之间访问coordinator时加锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task     // 使用chan保证并发安全
	MapTaskChannel    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
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
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
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
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	c.makeMapTasks(files)

	c.server()

	//监控worker工作情况
	go c.CrashDetector()

	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}
		for _, meta := range c.taskMetaHolder.MetaMap {
			if meta.state == Working && time.Since(meta.StartTime) > 9*time.Second {
				switch meta.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- meta.TaskAdr
					meta.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- meta.TaskAdr
					meta.state = Waiting

				}
			}
		}
		mu.Unlock()
	}
}

// 对map任务进行处理,初始化map任务
func (c *Coordinator) makeMapTasks(files []string) {
	for _, i := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			FileSlice:  []string{i},
			ReducerNum: c.ReducerNum,
		}
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		c.MapTaskChannel <- &task
	}
}

// 将接受taskMetaInfo储存进MetaHolder里
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	mate, _ := t.MetaMap[TaskInfo.TaskAdr.TaskId]
	if mate == nil {
		t.MetaMap[TaskInfo.TaskAdr.TaskId] = TaskInfo
	}
	return true
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId += 1
	return res
}

// 分发任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				c.taskMetaHolder.MetaMap[reply.TaskId].StartTime = time.Now()
				c.taskMetaHolder.MetaMap[reply.TaskId].state = Working
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
			return nil
		}
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				c.taskMetaHolder.MetaMap[reply.TaskId].StartTime = time.Now()
				c.taskMetaHolder.MetaMap[reply.TaskId].state = Working
			} else {
				reply.TaskType = WaittingTask
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
			}
			return nil
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
		fmt.Printf("finished MapPhase.")
	} else {
		c.DistPhase = AllDone
		fmt.Printf("finished ReducePhase.")
	}
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapdo      = 0
		reducedo   = 0
		mapundo    = 0
		reduceundo = 0
	)
	for _, v := range t.MetaMap {
		if v.state == Done {
			if v.TaskAdr.TaskType == MapTask {
				mapdo += 1
			} else {
				reducedo += 1
			}
		} else {
			if v.TaskAdr.TaskType == MapTask {
				mapundo += 1
			} else {
				reduceundo += 1
			}
		}
	}
	if (mapdo > 0 && mapundo == 0 && reducedo == 0 && reduceundo == 0) || (mapdo > 0 && mapundo == 0 && reduceundo == 0 && reduceundo > 0) {
		return true
	} else {
		return false
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		task := Task{
			TaskType:   ReduceTask,
			TaskId:     c.generateTaskId(),
			ReducerNum: c.ReducerNum,
			FileSlice:  selectReduceName(i),
		}
		c.ReduceTaskChannel <- &task
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
	if ok && meta.state == Working {
		meta.state = Done
	}

	return nil
}
