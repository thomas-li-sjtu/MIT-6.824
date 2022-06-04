package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation  string // "get" "put" "append"
	Key        string
	Value      string
	Client_id  int64 // 发送Op的client（一个请求的id和发送请求的client共同确定一个唯一性的请求）
	Request_id int   // 同一个请求要有唯一的id
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft // 每个KVServer对应一个Raft peer node（将raft作为一个服务）
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv_database             map[string]string // 存储key value的数据库
	wait_apply_ch           map[int]chan Op   // index of raft -> chan Op (key为一个请求在raft中的index，value为 Op通道)
	last_request_id         map[int64]int     // key:client id, value: request id
	last_snapshot_log_index int               // last SnapShot point , raftIndex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, is_leader := kv.rf.GetState()
	if !is_leader { // 被client RPC调用的该kvserver不是leader，则数据不一定是最新的，因此reply不为ok
		reply.Err = ErrWrongLeader
		return
	}

	// 根据args构建Op，由kv.rf.Start调用
	op := Op{
		Operation:  "get",
		Key:        args.Key,
		Value:      "",
		Client_id:  args.Client_id,
		Request_id: args.Request_id,
	}
	raft_log_index, _, _ := kv.rf.Start(op)

	// 构建wait channel
	kv.mu.Lock()
	wait_ch, exist := kv.wait_apply_ch[raft_log_index]
	if !exist {
		kv.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		wait_ch = kv.wait_apply_ch[raft_log_index]
	}
	kv.mu.Unlock()

	select {
	// select 随机执行一个可运行的 case。如果没有 case 可运行，它将阻塞，直到有 case 可运行
	// case 必须是一个通信操作，要么是发送要么是接收
	case <-time.After(time.Millisecond * 500): // 超时
		_, is_leader := kv.rf.GetState()
		if kv.Is_Duplicate(op.Client_id, op.Request_id) && is_leader {
			value, exist := kv.Execute_Get(op) // Get_Command_From_Raft没有执行Get，因此这里要主动Get
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case op_committed := <-wait_ch: // 此时，该kvserver运行了Get_Command_From_Raft().Send_Wait_Chan()
		if op_committed.Client_id == op.Client_id && op_committed.Request_id == op.Request_id {
			value, exist := kv.Execute_Get(op)
			if exist {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.wait_apply_ch, raft_log_index)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, is_leader := kv.rf.GetState()
	if !is_leader { // 被client RPC调用的该kvserver不是leader，则数据不一定是最新的，因此reply不为ok
		reply.Err = ErrWrongLeader
		return
	}

	// 根据args构建Op，由kv.rf.Start调用
	op := Op{
		Operation:  args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Client_id:  args.Client_id,
		Request_id: args.Request_id,
	}
	raft_log_index, _, _ := kv.rf.Start(op)

	// 构建wait channel
	kv.mu.Lock()
	wait_ch, exist := kv.wait_apply_ch[raft_log_index]
	if !exist {
		kv.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		wait_ch = kv.wait_apply_ch[raft_log_index]
	}
	kv.mu.Unlock()

	select {
	// select 随机执行一个可运行的 case。如果没有 case 可运行，它将阻塞，直到有 case 可运行
	// case 必须是一个通信操作，要么是发送要么是接收
	case <-time.After(time.Millisecond * 500): // 超时
		_, is_leader := kv.rf.GetState()
		if kv.Is_Duplicate(op.Client_id, op.Request_id) && is_leader {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case op_committed := <-wait_ch: // 此时，该kvserver运行了Get_Command_From_Raft().Send_Wait_Chan()，收到了
		DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId %d, Opreation %v, Key :%v, Value :%v", kv.me, raft_log_index, op.Client_id, op.Request_id, op.Operation, op.Key, op.Value)
		if op_committed.Client_id == op.Client_id && op_committed.Request_id == op.Request_id {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.wait_apply_ch, raft_log_index)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kv_database = make(map[string]string)
	kv.last_request_id = make(map[int64]int)
	kv.wait_apply_ch = make(map[int]chan Op)

	// TODO: 载入snapshot

	go kv.Read_Raft_Apply_Command()

	return kv
}

func (kv *KVServer) Read_Raft_Apply_Command() {
	for message := range kv.applyCh {
		// listen to every command applied by its raft ,delivery to relative RPC Handler
		if message.CommandValid {
			kv.Get_Command_From_Raft(message)
		}
		if message.SnapshotValid {
			fmt.Println("not implemented")
		}
	}
}

func (kv *KVServer) Get_Command_From_Raft(message raft.ApplyMsg) {
	// leader（每个kvserver都是一个raft）执行put和append命令
	op := message.Command.(Op)

	// if message.CommandIndex <= kv.last_snapshot_log_index { // 当前命令已经被snapshot了
	// 	return
	// }

	if !kv.Is_Duplicate(op.Client_id, op.Request_id) { // 判断这个request（op）是否重复
		// leader执行put和append，get则可以由任意副本执行
		switch op.Operation {
		case "Put":
			kv.Execute_Put(op)
		case "Append":
			kv.Execute_Append(op)
		}
	}

	if kv.maxraftstate != -1 {
		//TODO 判断是否要snapshot
	}

	kv.Send_Wait_Chan(op, message.CommandIndex)
	// Server(leader)有一个WaitChannel，等待Raft leader返回给自己Request结果
}

func (kv *KVServer) Is_Duplicate(client_id int64, request_id int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	last_request_id, exist := kv.last_request_id[client_id]
	if !exist {
		return false // 如果kv的记录中不存在这个client id，说明这个request一定不会重复
	}
	// 如果kv的记录中存在这个client id，那么这个client保存的最后一个requestid要小于当前request，才不会重复
	return last_request_id >= request_id
}

func (kv *KVServer) Send_Wait_Chan(op Op, command_index int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	channel, exist := kv.wait_apply_ch[command_index]
	if exist {
		channel <- op
	}
	return exist
}

func (kv *KVServer) Execute_Put(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kv_database[op.Key] = op.Value // 更新数据库的value
	kv.last_request_id[op.Client_id] = op.Request_id

	DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.Client_id, op.Request_id, op.Key, op.Value)

	return kv.kv_database[op.Key], true
}

func (kv *KVServer) Execute_Append(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exist := kv.kv_database[op.Key]
	if exist {
		kv.kv_database[op.Key] = value + op.Value
	} else {
		kv.kv_database[op.Key] = op.Value
	}
	kv.last_request_id[op.Client_id] = op.Request_id

	return kv.kv_database[op.Key], exist
}

func (kv *KVServer) Execute_Get(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exist := kv.kv_database[op.Key]
	kv.last_request_id[op.Client_id] = op.Request_id // kvserver更新request的记录（对于某个client id）

	if exist {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.Client_id, op.Request_id, op.Key, value)
	} else {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.Client_id, op.Request_id, op.Key)
	}
	return value, exist
}
