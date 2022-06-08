package shardctrler

import (
	"bytes"
	"log"
	"sync"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	maxraftstate            int
	configs                 []Config        // indexed by config num
	wait_apply_ch           map[int]chan Op // index of raft -> chan Op (key为一个请求在raft中的index，value为 Op通道)
	last_request_id         map[int64]int   // key:client id, value: request id
	last_snapshot_log_index int             // last SnapShot
}

type Op struct {
	// Your data here.
	Operation string // "query" "move" "join" "leave"

	Shard_Move   int              // 要移动的分片的id
	Gid_Move     int              // 要移动的分片所在的group id
	Index_Query  int              // 要查询的config下标
	Servers_Join map[int][]string // 加入
	Gids_Leave   []int

	Client_id  int64 // 发送Op的client（一个请求的id和发送请求的client共同确定一个唯一性的请求）
	Request_id int   // 同一个请求要有唯一的id
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if _, is_leader := sc.rf.GetState(); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Operation:    "join",
		Client_id:    args.Client_id,
		Request_id:   args.Request_id,
		Servers_Join: args.Servers,
	}
	raft_log_index, _, _ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	channel, exist := sc.wait_apply_ch[raft_log_index]
	if !exist {
		sc.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = sc.wait_apply_ch[raft_log_index]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 100):
		// DPrintf("[Leave TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d", args.Client_id, args.Request_id, sc.me, raft_log_index)
		_, is_leader := sc.rf.GetState()
		if sc.Is_Duplicate(op.Client_id, op.Request_id) && is_leader {
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

	sc.mu.Lock()
	delete(sc.wait_apply_ch, raft_log_index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if _, is_leader := sc.rf.GetState(); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Operation:  "leave",
		Client_id:  args.Client_id,
		Request_id: args.Request_id,
		Gids_Leave: args.GIDs,
	}
	raft_log_index, _, _ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	channel, exist := sc.wait_apply_ch[raft_log_index]
	if !exist {
		sc.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = sc.wait_apply_ch[raft_log_index]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 100): // 超时
		// DPrintf("[Leave TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d", args.Client_id, args.Request_id, sc.me, raft_log_index)
		_, is_leader := sc.rf.GetState()
		if sc.Is_Duplicate(op.Client_id, op.Request_id) && is_leader {
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

	sc.mu.Lock()
	delete(sc.wait_apply_ch, raft_log_index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if _, is_leader := sc.rf.GetState(); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Operation:  "move",
		Client_id:  args.Client_id,
		Request_id: args.Request_id,
		Shard_Move: args.Shard,
		Gid_Move:   args.GID,
	}
	raft_log_index, _, _ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	channel, exist := sc.wait_apply_ch[raft_log_index]
	if !exist {
		sc.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = sc.wait_apply_ch[raft_log_index]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(time.Millisecond * 100):
		// DPrintf("[Leave TIMEOUT]From Client %d (Request %d) To Server %d, raftIndex %d", args.Client_id, args.Request_id, sc.me, raft_log_index)
		_, is_leader := sc.rf.GetState()
		if sc.Is_Duplicate(op.Client_id, op.Request_id) && is_leader {
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

	sc.mu.Lock()
	delete(sc.wait_apply_ch, raft_log_index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if _, ifLeader := sc.rf.GetState(); !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Operation:   "query",
		Client_id:   args.Client_id,
		Request_id:  args.Request_id,
		Index_Query: args.Num,
	}
	raft_log_index, _, _ := sc.rf.Start(op)

	// create WaitForCh
	sc.mu.Lock()
	channel, exist := sc.wait_apply_ch[raft_log_index]
	if !exist {
		sc.wait_apply_ch[raft_log_index] = make(chan Op, 1)
		channel = sc.wait_apply_ch[raft_log_index]
	}
	sc.mu.Unlock()

	select {
	case <-time.After(100 * time.Millisecond):
		_, is_leader := sc.rf.GetState()
		if sc.Is_Duplicate(op.Client_id, op.Request_id) && is_leader {
			reply.Config = sc.Execute_Query(op)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case raftCommitOp := <-channel:
		if raftCommitOp.Client_id == op.Client_id && raftCommitOp.Request_id == op.Request_id {
			reply.Config = sc.Execute_Query(op)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.wait_apply_ch, raft_log_index)
	sc.mu.Unlock()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.last_request_id = make(map[int64]int)
	sc.wait_apply_ch = make(map[int]chan Op)
	sc.maxraftstate = -1

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sc.ReadSnapShotToInstall(snapshot)
	}

	go sc.Read_Raft_Apply_Command()

	return sc
}

func (sc *ShardCtrler) Read_Raft_Apply_Command() {
	for message := range sc.applyCh {
		// listen to every command applied by its raft ,delivery to relative RPC Handler
		if message.CommandValid {
			sc.Get_Command_From_Raft(message)
		}
		if message.SnapshotValid {
			sc.GetSnapShotFromRaft(message)
			// fmt.Println("not implemented")
		}
	}
}

func (sc *ShardCtrler) Get_Command_From_Raft(message raft.ApplyMsg) {
	// leader（每个kvserver都是一个raft）执行put和append命令
	op := message.Command.(Op)

	if message.CommandIndex <= sc.last_snapshot_log_index { // 当前命令已经被snapshot了
		return
	}

	if !sc.Is_Duplicate(op.Client_id, op.Request_id) { // 判断这个request（op）是否重复
		// leader执行put和append，get则可以由任意副本执行
		switch op.Operation {
		case "join":
			sc.Execute_Join(op)
		case "leave":
			sc.Execute_Leave(op)
		case "move":
			sc.Execute_Move(op)
		}
	}

	if sc.maxraftstate != -1 {
		sc.IfNeedToSendSnapShotCommand(message.CommandIndex, 9)
	}

	sc.Send_Wait_Chan(op, message.CommandIndex)
	// Server(leader)有一个WaitChannel，等待Raft leader返回给自己Request结果
}

func (sc *ShardCtrler) Execute_Query(op Op) Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.last_request_id[op.Client_id] = op.Request_id
	if op.Index_Query == -1 || op.Index_Query >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[op.Index_Query]
}

func (sc *ShardCtrler) Execute_Join(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.last_request_id[op.Client_id] = op.Request_id
	sc.configs = append(sc.configs, *sc.Join_Config(op.Servers_Join))
}

func (sc *ShardCtrler) Execute_Leave(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.last_request_id[op.Client_id] = op.Request_id
	sc.configs = append(sc.configs, *sc.Leave_Config(op.Gids_Leave))

}

func (sc *ShardCtrler) Execute_Move(op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.last_request_id[op.Client_id] = op.Request_id
	sc.configs = append(sc.configs, *sc.Move_Config(op.Shard_Move, op.Gid_Move))
}

func (sc *ShardCtrler) Is_Duplicate(client_id int64, request_id int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	last_request_id, exist := sc.last_request_id[client_id]
	if !exist {
		return false // 如果kv的记录中不存在这个client id，说明这个request一定不会重复
	}
	// 如果kv的记录中存在这个client id，那么这个client保存的最后一个requestid要小于当前request，才不会重复
	return last_request_id >= request_id
}

func (sc *ShardCtrler) Join_Config(servers_join map[int][]string) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]

	temp_groups := make(map[int][]string)
	for gid, server_list := range lastConfig.Groups { // 复制最后一个config的groups
		temp_groups[gid] = server_list
	}
	for gids, join_list := range servers_join { // 将新的serers加入集群
		temp_groups[gids] = join_list
	}

	group_shardnum := make(map[int]int) // 记录最后一个config中，每个group下分别有多少个shard；key：group_id value：shard_id
	for gid := range temp_groups {
		group_shardnum[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			group_shardnum[gid]++
		}
	}

	if len(group_shardnum) == 0 { // 如果上一个config为初始config
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: temp_groups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(group_shardnum, lastConfig.Shards),
		Groups: temp_groups,
	}
}

func (sc *ShardCtrler) Leave_Config(gid_leave []int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]

	group_del := make(map[int]bool) // 记录删除的group_id
	for _, gid := range gid_leave {
		group_del[gid] = true
	}

	temp_groups := make(map[int][]string)
	for gid, serverList := range lastConfig.Groups {
		temp_groups[gid] = serverList
	}
	for _, cur_gid := range gid_leave { // 通过group_id删除group
		delete(temp_groups, cur_gid)
	}

	// 得到delete group之后的 group_id:shard_num 映射，和 shard_id:group_id 映射
	newShard := lastConfig.Shards
	group_shardnum := make(map[int]int)
	for gid := range temp_groups { // 初始化 group_id:shard_num 映射
		if !group_del[gid] {
			group_shardnum[gid] = 0
		}
	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if group_del[gid] {
				newShard[shard] = 0 // shard所在的group要被删除，则记录归零
			} else {
				group_shardnum[gid]++
			}
		}
	}

	if len(group_shardnum) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: temp_groups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(group_shardnum, newShard),
		Groups: temp_groups,
	}
}

func (sc *ShardCtrler) Move_Config(shard, gid int) *Config {
	last_config := sc.configs[len(sc.configs)-1]
	cur_config := Config{
		Num:    last_config.Num + 1, // len(sc.configs)
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	// 复制一遍最后一个config，然后再微调
	for shards, gids := range last_config.Shards {
		cur_config.Shards[shards] = gids
	}
	for gidss, servers := range last_config.Groups {
		cur_config.Groups[gidss] = servers
	}
	cur_config.Shards[shard] = gid

	return &cur_config
}

func (sc *ShardCtrler) Send_Wait_Chan(op Op, command_index int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	channel, exist := sc.wait_apply_ch[command_index]
	if exist {
		channel <- op
	}
	return exist
}

func (sc *ShardCtrler) reBalanceShards(groupid_shardnum map[int]int, last_shards [NShards]int) [NShards]int {
	// 根据group_id：shard_num映射，和shard_id：group_id映射，重新平衡负载
	group_num := len(groupid_shardnum)
	avg_shard_per_group := NShards / group_num
	subNum := NShards % group_num // 无法均匀分配的shard数目
	realSortNum := realNumArray(groupid_shardnum)

	//多退
	for i := group_num - 1; i >= 0; i-- {
		resultNum := avg_shard_per_group

		var is_avg bool // 将无法均匀分配的剩余shard，放到realSortNum中后面那几个group中，每个group分一个
		if i < group_num-subNum {
			is_avg = true
		} else {
			is_avg = false
		}
		if !is_avg {
			resultNum = avg_shard_per_group + 1
		}

		if resultNum < groupid_shardnum[realSortNum[i]] {
			fromGid := realSortNum[i]                          // 从这个group转移出changeNum个shard
			changeNum := groupid_shardnum[fromGid] - resultNum // 要转移的shard数目
			for shard, gid := range last_shards {
				if changeNum <= 0 {
					break
				}
				if gid == fromGid {
					last_shards[shard] = 0 // fromGid的这个shard高于avg_shard_per_group，要“退”，退到group 0暂存
					changeNum -= 1
				}
			}
			groupid_shardnum[fromGid] = resultNum
		}
	}

	//少补
	for i := 0; i < group_num; i++ {
		resultNum := avg_shard_per_group

		var flag bool
		if i < avg_shard_per_group-subNum {
			flag = true
		} else {
			flag = false
		}
		if !flag {
			resultNum = avg_shard_per_group + 1
		}

		if resultNum > groupid_shardnum[realSortNum[i]] {
			toGid := realSortNum[i]
			changeNum := resultNum - groupid_shardnum[toGid]
			for shard, gid := range last_shards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					last_shards[shard] = toGid // fromGid的这个shard少于avg_shard_per_group，要“补”，从group 0中补
					changeNum -= 1
				}
			}
			groupid_shardnum[toGid] = resultNum
		}
	}
	return last_shards
}

// 根据group_id下shard的数目升序排序group_id，如果shard数目相同，则group_id小的在前面
func realNumArray(groupid_shardnum map[int]int) []int {
	group_num := len(groupid_shardnum)

	numArray := make([]int, 0, group_num) // length 0, capacity 10
	for group_id, _ := range groupid_shardnum {
		numArray = append(numArray, group_id)
	}

	for i := 0; i < group_num-1; i++ {
		for j := group_num - 1; j > i; j-- {
			if groupid_shardnum[numArray[j]] < groupid_shardnum[numArray[j-1]] || (groupid_shardnum[numArray[j]] == groupid_shardnum[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1], numArray[j]
			}
		}
	}
	return numArray
}

func (sc *ShardCtrler) IfNeedToSendSnapShotCommand(raftIndex int, proportion int) {
	if sc.rf.GetRaftStateSize() > (sc.maxraftstate * proportion / 10) {
		// Send SnapShot Command
		snapshot := sc.MakeSnapShot()
		sc.rf.Snapshot(raftIndex, snapshot)
	}
}

// Handler the SnapShot from kv.rf.applyCh
func (sc *ShardCtrler) GetSnapShotFromRaft(message raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
		sc.ReadSnapShotToInstall(message.Snapshot)
		sc.last_snapshot_log_index = message.SnapshotIndex
	}
}

// Give it to raft when server decide to start a snapshot
func (sc *ShardCtrler) MakeSnapShot() []byte {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.configs)
	e.Encode(sc.last_request_id)
	data := w.Bytes()
	return data
}

func (sc *ShardCtrler) ReadSnapShotToInstall(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persist_config []Config
	var persist_lastRequestId map[int64]int

	if d.Decode(&persist_config) != nil || d.Decode(&persist_lastRequestId) != nil {
		DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!", sc.me)
	} else {
		sc.configs = persist_config
		sc.last_request_id = persist_lastRequestId
	}
}
