package shardkv

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
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
	Operation  string
	Key        string
	Value      string
	Client_id  int64
	Request_id int

	New_config         shardctrler.Config
	Migrate_data       []Shard
	Config_num_migrate int
}

type Shard struct {
	Shard_id        int
	Kv_data         map[string]string
	Last_request_id map[int64]int // 以shard为单位记录最后一个访问该shard的id
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // 所属的group
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck                     *shardctrler.Clerk // 作为shardctrler.Clerk向shardCtriler获取新的config
	ctrl_config             shardctrler.Config // 存储配置
	kv_database             []Shard            // 以shard为单位存储
	wait_apply_ch           map[int]chan Op    // index of raft -> chan Op (key为一个请求在raft中的index，value为 Op通道)
	last_snapshot_log_index int
	migrating_shard         [shardctrler.NShards]bool // 如果第i个shard迁移了，则相应第i个元素为true
}

func (kv *ShardKV) is_shard_in_group(config_num, shard_id int) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// client的config num是否和kvserver的config num相同、shard是否在这个group中； shard是否在迁移——是否可用
	return kv.ctrl_config.Num == config_num && kv.ctrl_config.Shards[shard_id] == kv.gid, !kv.migrating_shard[shard_id]
}

// 检查要migrate的shard是否已经迁移完成（检查args的shard list在migrating_shard是否为false）
func (kv *ShardKV) check_shard_been_migrated(shard_list []Shard) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, cur_shard := range shard_list {
		if kv.migrating_shard[cur_shard.Shard_id] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if _, is_leader := kv.rf.GetState(); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}

	shard_id := key2shard(args.Key)
	is_in_group, is_available := kv.is_shard_in_group(args.Config_num, shard_id)
	if !is_in_group {
		reply.Err = ErrWrongGroup
		return
	}
	if !is_available {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:  "Get",
		Client_id:  args.Client_id,
		Request_id: args.Request_id,
		Key:        args.Key,
		Value:      "",
	}
	raft_log_index, _, _ := kv.rf.Start(op)

	// create WaitForCh
	kv.mu.Lock()
	channel, exist := kv.wait_apply_ch[raft_log_index]
	if !exist {
		kv.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = kv.wait_apply_ch[raft_log_index]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 500): // 超时
		// DPrintf("[Leave TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d", args.Client_id, args.Request_id, sc.me, raft_log_index)
		_, is_leader := kv.rf.GetState()
		shard_id := key2shard(op.Key)
		if kv.Is_Duplicate(op.Client_id, op.Request_id, shard_id) && is_leader {
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
	case raftCommitOp := <-channel:
		if raftCommitOp.Client_id == op.Client_id && raftCommitOp.Request_id == op.Request_id {
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
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if _, is_leader := kv.rf.GetState(); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}

	shard_id := key2shard(args.Key)
	is_in_group, is_available := kv.is_shard_in_group(args.Config_num, shard_id)
	if !is_in_group {
		reply.Err = ErrWrongGroup
		return
	}
	if !is_available {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation:  args.Op,
		Client_id:  args.Client_id,
		Request_id: args.Request_id,
		Key:        args.Key,
		Value:      args.Value,
	}
	raft_log_index, _, _ := kv.rf.Start(op)

	// create WaitForCh
	kv.mu.Lock()
	channel, exist := kv.wait_apply_ch[raft_log_index]
	if !exist {
		kv.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = kv.wait_apply_ch[raft_log_index]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 500): // 超时
		// DPrintf("[Leave TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d", args.Client_id, args.Request_id, sc.me, raft_log_index)
		_, is_leader := kv.rf.GetState()
		shard_id := key2shard(op.Key)
		if kv.Is_Duplicate(op.Client_id, op.Request_id, shard_id) && is_leader {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channel:
		if raftCommitOp.Client_id == op.Client_id && raftCommitOp.Request_id == op.Request_id {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.wait_apply_ch, raft_log_index)
	kv.mu.Unlock()
}

// migrate rpc
func (kv *ShardKV) MigrateShard(args *MigrateShardArgs, reply *MigrateShardReply) {
	kv.mu.Lock()
	cur_config_num := kv.ctrl_config.Num
	kv.mu.Unlock()

	// 收到RPC时，kvserver的config必须和发送的config时间一致
	if args.Config_num > cur_config_num { // 当前的config滞后，可能需要先更新数据，因此不迁移
		reply.Err = ErrConfigNum
		reply.Config_num = cur_config_num
		return
	}
	if args.Config_num < cur_config_num || kv.check_shard_been_migrated(args.Migrate_data) {
		reply.Err = OK
		return
	}

	op := Op{
		Operation:          "migrate",
		Migrate_data:       args.Migrate_data,
		Config_num_migrate: args.Config_num,
	}
	raft_log_index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	channel, exist := kv.wait_apply_ch[raft_log_index]
	if !exist {
		kv.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = kv.wait_apply_ch[raft_log_index]
	}
	kv.mu.Unlock()
	select {
	case <-time.After(time.Millisecond * 500):
		kv.mu.Lock()
		_, is_leader := kv.rf.GetState()
		cur_config_num := kv.ctrl_config.Num
		kv.mu.Unlock()

		if args.Config_num <= cur_config_num && kv.check_shard_been_migrated(args.Migrate_data) && is_leader {
			reply.Config_num = cur_config_num
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case op_committed := <-channel:
		kv.mu.Lock()
		cur_config_num := kv.ctrl_config.Num
		kv.mu.Unlock()
		if op_committed.Config_num_migrate == args.Config_num && args.Config_num <= cur_config_num && kv.check_shard_been_migrated(args.Migrate_data) {
			reply.Config_num = cur_config_num
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.wait_apply_ch, raft_log_index)
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.kv_database = make([]Shard, shardctrler.NShards)
	for shard_id := 0; shard_id < shardctrler.NShards; shard_id++ {
		kv.kv_database[shard_id] = Shard{
			Shard_id:        shard_id,
			Kv_data:         make(map[string]string),
			Last_request_id: make(map[int64]int),
		}
	}

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.wait_apply_ch = make(map[int]chan Op)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShotToInstall(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Read_Raft_Apply_Command()
	go kv.Get_New_Config()
	go kv.Detect_Migrate_Data()

	return kv
}

func (kv *ShardKV) Read_Raft_Apply_Command() {
	for message := range kv.applyCh {
		// listen to every command applied by its raft ,delivery to relative RPC Handler
		if message.CommandValid {
			kv.Get_Command_From_Raft(message)
		}
		if message.SnapshotValid {
			kv.GetSnapShotFromRaft(message)
		}
	}
}

func (kv *ShardKV) Get_New_Config() {
	for !kv.killed() {
		kv.mu.Lock()
		last_config_num := kv.ctrl_config.Num
		_, is_leader := kv.rf.GetState()
		kv.mu.Unlock()

		if !is_leader {
			time.Sleep(90 * time.Millisecond)
			continue
		}

		new_config := kv.mck.Query(last_config_num + 1) // 获得下一个config  注意，必须是下一个，而不是最新！即使当前的server滞后，必须按config顺序执行
		if new_config.Num == last_config_num+1 {
			kv.mu.Lock()
			op := Op{
				Operation:  "newconfig",
				New_config: new_config,
			}
			if _, ifLeader := kv.rf.GetState(); ifLeader {
				kv.rf.Start(op) // 更新所有的config
			}
			kv.mu.Unlock()
		}
		time.Sleep(90 * time.Millisecond)
	}
}

func (kv *ShardKV) Detect_Migrate_Data() {
	for !kv.killed() {
		kv.mu.Lock()
		_, is_leader := kv.rf.GetState()
		kv.mu.Unlock()

		if !is_leader {
			time.Sleep(120 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		no_migrate := true
		for _, need_migrate := range kv.migrating_shard {
			if need_migrate {
				no_migrate = false
				break
			}
		}
		kv.mu.Unlock()

		if no_migrate {
			time.Sleep(120 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		send_data := make(map[int][]Shard)                              // target group: shard list
		for shard_id := 0; shard_id < shardctrler.NShards; shard_id++ { // 记录发送出去的shard
			cur_group := kv.ctrl_config.Shards[shard_id]
			if kv.migrating_shard[shard_id] && kv.gid != cur_group { // 这些数据要送出去（shard被冻结，并且shard所属的group和kvserver不同。注意，这里的config已经是最新的了）
				tmp_shard := Shard{
					Shard_id:        shard_id,
					Kv_data:         make(map[string]string),
					Last_request_id: make(map[int64]int),
				}
				// 将要发送的数据复制出来
				for key, value := range kv.kv_database[shard_id].Kv_data {
					tmp_shard.Kv_data[key] = value
				}
				for client_id, request_id := range kv.kv_database[shard_id].Last_request_id {
					tmp_shard.Last_request_id[client_id] = request_id
				}
				send_data[cur_group] = append(send_data[cur_group], tmp_shard)
			}
		}
		kv.mu.Unlock()
		if len(send_data) == 0 { // 如果没有要送出去的数据
			time.Sleep(120 * time.Millisecond)
			continue
		}

		//Send Data
		for target_gid, shards_to_send := range send_data {
			kv.mu.Lock()
			target_servers := kv.ctrl_config.Groups[target_gid]
			args := &MigrateShardArgs{
				Migrate_data: make([]Shard, 0),
				Config_num:   kv.ctrl_config.Num,
			}
			kv.mu.Unlock()

			for _, cur_shard := range shards_to_send {
				tmp_shard := Shard{
					Shard_id:        cur_shard.Shard_id,
					Kv_data:         make(map[string]string),
					Last_request_id: make(map[int64]int),
				}
				// 将要发送的数据装进来
				for key, value := range cur_shard.Kv_data {
					tmp_shard.Kv_data[key] = value
				}
				for client_id, request_id := range cur_shard.Last_request_id {
					tmp_shard.Last_request_id[client_id] = request_id
				}
				args.Migrate_data = append(args.Migrate_data, tmp_shard)
			}

			// 分别向不同的group发送shard list
			go func(groupServers []string, args *MigrateShardArgs) {
				for _, groupMember := range groupServers {
					callEnd := kv.make_end(groupMember)
					migrateReply := MigrateShardReply{}
					ok := callEnd.Call("ShardKV.MigrateShard", args, &migrateReply)

					kv.mu.Lock()
					cur_config_num := kv.ctrl_config.Num
					kv.mu.Unlock()

					if ok && migrateReply.Err == OK {
						// Send to Raft : I'have Send my Components out
						if cur_config_num != args.Config_num || kv.check_shard_been_migrated(args.Migrate_data) {
							return
						} else {
							kv.rf.Start(
								Op{
									Operation:          "migrate",
									Migrate_data:       args.Migrate_data,
									Config_num_migrate: args.Config_num,
								},
							)
							return
						}
					}
				}
			}(target_servers, args)
		}
		time.Sleep(120 * time.Millisecond)
	}
}

func (kv *ShardKV) Get_Command_From_Raft(message raft.ApplyMsg) {
	// leader（每个kvserver都是一个raft）执行put和append命令，也有可能是向ctrler申请新的config、向其他group转移shard
	op := message.Command.(Op)

	if message.CommandIndex <= kv.last_snapshot_log_index { // 当前命令已经被snapshot了
		return
	}

	if op.Operation == "migrate" {
		kv.Execute_Migrate(op)
		if kv.maxraftstate != -1 {
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
		}
		kv.Send_Wait_Chan(op, message.CommandIndex)
		return
	}

	if op.Operation == "newconfig" {
		kv.Execute_NewConfig(op)
		if kv.maxraftstate != -1 {
			kv.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
		}
		return
	}

	if !kv.Is_Duplicate(op.Client_id, op.Request_id, key2shard(op.Key)) { // 判断这个request（op）是否重复
		// leader执行put和append，get则可以由任意副本执行
		switch op.Operation {
		case "Put":
			kv.Execute_Put(op)
		case "Append":
			kv.Execute_Append(op)
		}
	}

	if kv.maxraftstate != -1 {
		kv.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	kv.Send_Wait_Chan(op, message.CommandIndex)
	// Server(leader)有一个WaitChannel，等待Raft leader返回给自己Request结果
}

func (kv *ShardKV) Execute_Put(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard_id := key2shard(op.Key)
	kv.kv_database[shard_id].Kv_data[op.Key] = op.Value // 更新数据库的value
	kv.kv_database[shard_id].Last_request_id[op.Client_id] = op.Request_id

	DPrintf("[KVServerExePUT----]ClientId :%d ,RequestID :%d ,Key : %v, value : %v", op.Client_id, op.Request_id, op.Key, op.Value)

	return kv.kv_database[shard_id].Kv_data[op.Key], true
}

func (kv *ShardKV) Execute_Append(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard_id := key2shard(op.Key)
	value, exist := kv.kv_database[shard_id].Kv_data[op.Key]
	if exist {
		kv.kv_database[shard_id].Kv_data[op.Key] = value + op.Value
	} else {
		kv.kv_database[shard_id].Kv_data[op.Key] = op.Value
	}
	kv.kv_database[shard_id].Last_request_id[op.Client_id] = op.Request_id

	return kv.kv_database[shard_id].Kv_data[op.Key], exist
}

func (kv *ShardKV) Execute_Get(op Op) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard_id := key2shard(op.Key)
	value, exist := kv.kv_database[shard_id].Kv_data[op.Key]
	kv.kv_database[shard_id].Last_request_id[op.Client_id] = op.Request_id // kvserver更新request的记录（对于某个client id）

	if exist {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, value :%v", op.Client_id, op.Request_id, op.Key, value)
	} else {
		DPrintf("[KVServerExeGET----]ClientId :%d ,RequestID :%d ,Key : %v, But No KEY!!!!", op.Client_id, op.Request_id, op.Key)
	}
	return value, exist
}

func (kv *ShardKV) Execute_NewConfig(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	new_config := op.New_config
	if new_config.Num != kv.ctrl_config.Num+1 {
		return
	}

	// 更新config时，所有的迁移应当停止
	for _, is_migrating := range kv.migrating_shard {
		if is_migrating {
			return
		}
	}

	// 记录那些shard需要迁移
	for shard_id := 0; shard_id < shardctrler.NShards; shard_id++ {
		// 不在当前group且不在空闲group的shard，接下来要放入group
		if kv.ctrl_config.Shards[shard_id] != kv.gid && new_config.Shards[shard_id] == kv.gid && kv.ctrl_config.Shards[shard_id] != 0 {
			kv.migrating_shard[shard_id] = true
		}

		// 在当前group、将移到非空闲group的shard，接下来要移出group
		if kv.ctrl_config.Shards[shard_id] == kv.gid && new_config.Shards[shard_id] != kv.gid && new_config.Shards[shard_id] != 0 {
			kv.migrating_shard[shard_id] = true
		}
	}
	kv.ctrl_config = new_config
}

func (kv *ShardKV) Execute_Migrate(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cur_config := kv.ctrl_config
	if op.Config_num_migrate != cur_config.Num {
		return
	}

	// apply the MigrateShardData
	for _, cur_shard := range op.Migrate_data {
		if !kv.migrating_shard[cur_shard.Shard_id] {
			continue
		}
		kv.migrating_shard[cur_shard.Shard_id] = false
		kv.kv_database[cur_shard.Shard_id] = Shard{
			Shard_id:        cur_shard.Shard_id,
			Kv_data:         make(map[string]string),
			Last_request_id: make(map[int64]int),
		}
		// 复制数据给自己
		if cur_config.Shards[cur_shard.Shard_id] == kv.gid {
			for key, value := range cur_shard.Kv_data {
				kv.kv_database[cur_shard.Shard_id].Kv_data[key] = value
			}
			for client_id, request_id := range cur_shard.Last_request_id {
				kv.kv_database[cur_shard.Shard_id].Last_request_id[client_id] = request_id
			}
		}
	}
}

func (kv *ShardKV) Send_Wait_Chan(op Op, command_index int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	channel, exist := kv.wait_apply_ch[command_index]
	if exist {
		channel <- op
	}
	return exist
}
