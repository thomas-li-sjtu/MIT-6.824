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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker_id := strconv.Itoa(os.Getpid())
	// Your worker implementation here.
	for {
		reply := Ask_reply{}
		args := Ask_args{
			Worker_id: worker_id,
			Task_type: "",
		}
		call_result := call("Coordinator.Ask_for_task", &args, &reply)
		time.Sleep(1 * time.Second)
		if !call_result || reply.Task_type == "done" { // call不成功，可能job已经结束
			break
		}
		if reply.Task_op == 0 {
			continue
		}
		switch reply.Task_type {
		case "map":
			file, err := os.Open(reply.File_name)
			if err != nil {
				log.Fatalf("cannot open %v", reply.File_name)
				return
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File_name)
				return
			}
			defer file.Close()

			key_value := mapf(reply.File_name, string(content))

			// 创建输出文件
			var encoders []*json.Encoder
			for i := 0; i < reply.Num_reduce; i++ {
				f, err := os.Create(fmt.Sprintf("mr-%d-%d", reply.Task_index, i))
				if err != nil {
					log.Fatalf("cannot create intermediate result file")
					return
				}
				encoders = append(encoders, json.NewEncoder(f))
			}
			// 写入中间结果
			for _, kv := range key_value {
				_ = encoders[ihash(kv.Key)%reply.Num_reduce].Encode(&kv)
			}

			call("Coordinator.Finish", &Finish_args{Worker_id: worker_id, Task_type: "map", Task_index: reply.Task_index}, &Finish_reply{})
		case "reduce":
			// 读取中间结果
			var intermediate []KeyValue
			for _, file_name := range reply.Intermediate_files {
				file, err := os.Open(file_name)
				if err != nil {
					log.Fatalf("cannot open intermediate file %v", file_name)
					return
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("cannot close %v", file_name)
					return
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.Task_index)
			temp, err := os.CreateTemp(".", oname)
			if err != nil {
				log.Fatalf("cannot create reduce result tempfile %s", oname)
				return
			}

			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(temp, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			err = os.Rename(temp.Name(), oname)
			if err != nil {
				return
			}

			call("Coordinator.Finish", &Finish_args{Worker_id: worker_id, Task_type: "reduce", Task_index: reply.Task_index}, &Finish_reply{})
		}
	}
}

// send an RPC request to the Coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
