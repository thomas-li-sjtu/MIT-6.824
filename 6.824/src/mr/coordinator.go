package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock             sync.Mutex
	num_map          int // 输入文件数目
	num_reduce       int
	cur_stage        string          //记录当前的工作状态（map/reduce）
	tasks_dis_record map[string]Task // 记录分配出去的task，以及所属的worker和deadline: key为cur_stage+分配的worker的pid
	tasks_to_distri  chan Task       // task池大小，max(n_map, n_reduce)
}

// Your code here -- RPC handlers for the worker to call.
func (m *Coordinator) Ask_for_task(args *Ask_args, reply *Ask_reply) error {
	m.lock.Lock()

	reply.Task_op = 0
	task_distribute, ok := <-m.tasks_to_distri // 通道里送出一个task
	if !ok {
		return nil
	}

	reply.File_name = task_distribute.file_name
	reply.Num_reduce = m.num_reduce
	reply.Task_type = m.cur_stage
	reply.Task_index = task_distribute.index
	reply.Task_deadline = time.Now().Add(10 * time.Second)
	reply.Worker_id = args.Worker_id
	reply.Intermediate_files = nil
	if m.cur_stage == "reduce" { // 中间文件名mr-X-Y，其中X是map任务号，Y是reduce任务
		for map_index := 0; map_index < m.num_map; map_index++ {
			reply.Intermediate_files = append(reply.Intermediate_files,
				fmt.Sprintf("mr-%d-%d", map_index, task_distribute.index))
		}
	}
	reply.Task_op = 1

	task_distribute.worker_id = args.Worker_id
	task_distribute.task_type = m.cur_stage
	task_distribute.deadline = reply.Task_deadline

	m.tasks_dis_record[m.cur_stage+" "+strconv.Itoa(task_distribute.index)] = task_distribute // 更新record

	m.lock.Unlock()

	return nil
}

func (m *Coordinator) Finish(args *Finish_args, reply *Finish_reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	index := args.Task_type + " " + strconv.Itoa(args.Task_index)
	fmt.Println(len(m.tasks_dis_record))
	if task, exists := m.tasks_dis_record[index]; exists && task.worker_id == args.Worker_id {
		delete(m.tasks_dis_record, index) // 从分配任务记录中删除
		if len(m.tasks_dis_record) == 0 { // 如果一个阶段任务已经完成
			if m.cur_stage == "map" { // map任务完成，生成reduce任务
				for i := 0; i < m.num_reduce; i++ {
					tmp_task := Task{
						index:     i,
						task_type: "reduce",
						file_name: "",
					}
					m.tasks_dis_record["reduce"+" "+strconv.Itoa(i)] = tmp_task
					m.tasks_to_distri <- tmp_task
				}
				time.Sleep(1 * time.Second)
				m.cur_stage = "reduce"
			} else if m.cur_stage == "reduce" { // reduce任务完成，关闭通道，stage为done
				close(m.tasks_to_distri)
				m.cur_stage = "done"
			}
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Coordinator) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
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
// main/mrCoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Coordinator) Done() bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	ret := false

	// Your code here.
	if m.cur_stage == "done" {
		ret = true
		time.Sleep(time.Second)
	}

	return ret
}

//
// create a Coordinator.
// main/mrCoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{}

	// Your code here.
	// 初始化Coordinator
	m.cur_stage = "map"
	m.num_map = len(files)
	m.num_reduce = nReduce
	var pool_size int
	if m.num_map < m.num_reduce {
		pool_size = m.num_reduce
	} else {
		pool_size = m.num_map
	}
	m.tasks_dis_record = make(map[string]Task)
	m.tasks_to_distri = make(chan Task, pool_size)

	// 给各个文件分配map
	for i, file_name := range files {
		tmp_task := Task{
			index:     i,
			task_type: "map",
			file_name: file_name,
		}
		m.tasks_dis_record["map"+" "+strconv.Itoa(i)] = tmp_task
		m.tasks_to_distri <- tmp_task
	}

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			m.lock.Lock()
			for _, task := range m.tasks_dis_record {
				if task.worker_id != "" && time.Now().After(task.deadline) {
					// 回收并重新分配
					log.Printf(
						"Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
						m.cur_stage, task.index, task.worker_id,
					)
					task.worker_id = ""
					m.tasks_to_distri <- task
				}
			}
			m.lock.Unlock()
		}
	}()

	m.server()
	return &m
}
