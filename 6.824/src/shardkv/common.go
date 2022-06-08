package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrConfigNum   = "ErrConfigNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client_id  int64
	Request_id int
	Config_num int // Config的id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client_id  int64
	Request_id int
	Config_num int
}

type GetReply struct {
	Err   Err
	Value string
}

//TODO: migrate reply and args
type MigrateShardArgs struct {
	Migrate_data []Shard
	Config_num   int
}

type MigrateShardReply struct {
	Err        Err
	Config_num int
}
