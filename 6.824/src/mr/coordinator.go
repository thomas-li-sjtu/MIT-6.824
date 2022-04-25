package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	lock        sync.Mutex
	num_map     int // 输入文件数目
	num_reduce  int
	cur_stage   string        //记录当前的工作状态（map/reduce）
	map_task    []Map_task    // map task
	reduce_task []Reduce_task //
}

// Your code here -- RPC handlers for the worker to call.
func (m *Coordinator) Ask_for_task(args *Ask_args, reply *Ask_reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	reply.Task_op = 0

	if m.cur_stage == "map" {
		// fmt.Printf("Coordinator current stage: %v\n", m.cur_stage)
		for i, temp_task := range m.map_task {
			if !temp_task.Distributed { // map task没有分配出去
				m.map_task[i].Distributed = true
				m.map_task[i].Deadline = time.Now().Add(10 * time.Second)
				m.map_task[i].Worker_id = args.Worker_id

				reply.File_name = temp_task.File_name
				reply.Num_reduce = m.num_reduce
				reply.Task_type = "map"
				reply.Task_index = temp_task.Index
				reply.Task_deadline = m.map_task[i].Deadline
				reply.Worker_id = args.Worker_id
				break
			}
		}
	} else if m.cur_stage == "reduce" { // m.cur_stage == "reduce"
		// fmt.Println("Coordinator current stage: " + m.cur_stage)
		for i, temp_task := range m.reduce_task {
			if !temp_task.Distributed { // map task没有分配出去
				m.reduce_task[i].Distributed = true
				m.reduce_task[i].Deadline = time.Now().Add(10 * time.Second)
				m.reduce_task[i].Worker_id = args.Worker_id

				reply.File_name = ""
				reply.Num_reduce = m.num_reduce
				reply.Task_type = "reduce"
				reply.Task_index = temp_task.Index
				reply.Task_deadline = m.reduce_task[i].Deadline
				reply.Worker_id = args.Worker_id

				reply.Intermediate_files = nil // 中间文件名mr-X-Y，其中X是map任务号，Y是reduce任务
				for map_index := 0; map_index < m.num_map; map_index++ {
					reply.Intermediate_files = append(reply.Intermediate_files, fmt.Sprintf("mr-%d-%d", map_index, temp_task.Index))
				}
				break
			}
		}
	} else if m.cur_stage == "done" {
		reply.Task_op = 0
		return nil
	} else {
		// fmt.Println("Wrong Coordinator current stage: " + m.cur_stage)
		return nil
	}

	reply.Task_op = 1
	return nil
}

func (m *Coordinator) Finish(args *Finish_args, reply *Finish_reply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.cur_stage == "map" {
		// fmt.Println("A map task finished")
		if args.Task_type != "map" {
			fmt.Printf("Expect map type finish message, got %s type", args.Task_type)
			return nil
		}
		if m.map_task[args.Task_index].Distributed &&
			m.map_task[args.Task_index].Worker_id == args.Worker_id &&
			m.map_task[args.Task_index].Map_done == 0 &&
			time.Now().Before(m.map_task[args.Task_index].Deadline) {
			// fmt.Printf("map task %v is done\n", args.Task_index)
			m.map_task[args.Task_index].Map_done = 1
			for _, tmp_task := range m.map_task {
				if tmp_task.Map_done == 0 { // 如果有一个map task任务没有完成，则继续进行
					return nil
				}
			}
			time.Sleep(1 * time.Second)
			m.cur_stage = "reduce"

			fmt.Println("Start to reduce")
		}
	} else if m.cur_stage == "reduce" {
		// fmt.Println("A reduce task finished")
		if args.Task_type != "reduce" {
			fmt.Printf("Expect reduce type finish message, got %v type \n", args.Task_type)
			return nil
		}
		if m.reduce_task[args.Task_index].Distributed &&
			m.reduce_task[args.Task_index].Worker_id == args.Worker_id &&
			m.reduce_task[args.Task_index].Reduce_done == 0 &&
			time.Now().Before(m.reduce_task[args.Task_index].Deadline) {
			// fmt.Printf("reduce task %v is done \n", args.Task_index)
			m.reduce_task[args.Task_index].Reduce_done = 1
			for _, tmp_task := range m.reduce_task {
				if tmp_task.Reduce_done == 0 { // 如果有一个reduce task任务没有完成，则继续进行
					return nil
				}
			}
			time.Sleep(1 * time.Second)
			m.cur_stage = "done"

			fmt.Println("done")
		}
	} else {
		return nil
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrCoordinator.go calls Done() periodically to find out
// if the entire job has finished.
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

// create a Coordinator.
// main/mrCoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{}

	// Your code here.
	// 初始化Coordinator
	m.cur_stage = "map"
	m.num_map = len(files)
	m.num_reduce = nReduce
	m.map_task = make([]Map_task, m.num_map)
	m.reduce_task = make([]Reduce_task, m.num_reduce)

	for i, file_name := range files { // map任务加入列表
		tmp_task := Map_task{
			Index:       i,
			Worker_id:   "",
			File_name:   file_name,
			Map_done:    0,
			Distributed: false,
		}
		m.map_task[i] = tmp_task
	}
	for i := 0; i < m.num_reduce; i++ { // reduce任务加入列表
		tmp_task := Reduce_task{
			Index:              i,
			Worker_id:          "",
			Intermediate_files: make([]string, m.num_map),
			Reduce_done:        0,
			Distributed:        false,
		}
		m.reduce_task[i] = tmp_task
	}

	m.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			m.lock.Lock()
			if m.cur_stage == "map" {
				for i, task := range m.map_task {
					if task.Worker_id != "" &&
						task.Distributed &&
						time.Now().After(task.Deadline) &&
						task.Map_done == 0 { // 回收并重新分配
						// log.Printf(
						// 	"Found timed-out %s task %d previously on worker %s. Re-assign",
						// 	m.cur_stage, task.Index, task.Worker_id,
						// )
						m.map_task[i].Worker_id = ""
						m.map_task[i].Distributed = false
						m.map_task[i].Map_done = 0
					}
				}
			} else {
				for i, task := range m.reduce_task {
					if task.Worker_id != "" &&
						task.Distributed &&
						time.Now().After(task.Deadline) &&
						task.Reduce_done == 0 { // 回收并重新分配
						// log.Printf(
						// 	"Found timed-out %s task %d previously on worker %s. Re-assign",
						// 	m.cur_stage, task.Index, task.Worker_id,
						// )
						m.reduce_task[i].Worker_id = ""
						m.reduce_task[i].Distributed = false
						m.reduce_task[i].Reduce_done = 0
					}
				}
			}
			m.lock.Unlock()
		}
	}()
	return &m
}
