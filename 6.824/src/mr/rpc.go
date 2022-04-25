package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	index     int    // 当前task号（即第i个文件）
	worker_id string // 当前task所属worker的id
	task_type string
	file_name string
	deadline  time.Time
}

type Ask_args struct { // worker申请时，向Coordinator发送的args
	Worker_id string // 做申请的worker id
	Task_type string // 上一个完成的task的类型
}

type Ask_reply struct { // Coordinator收到worker申请时，回复的reply
	File_name          string
	Num_reduce         int
	Task_op            int
	Task_type          string
	Task_index         int // map任务时，输出的文件名为mr-Task_index-0, ~-1, ..., ~-(Num_reduce-1)
	Task_deadline      time.Time
	Worker_id          string
	Intermediate_files []string // 分配reduce任务时，对应的map中间文件名列表
}

type Finish_args struct { // worker完成时，向Coordinator发送的args
	Worker_id  string // 发送结束信号的worker进程号
	Task_type  string // worker完成的任务类型
	Task_index int    // 完成的task id
}

type Finish_reply struct { // Coordinator收到worker完成消息时，回复的reply（不需要reply，所以为空）
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the Coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
