package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"

	mathrand "math/rand"
)

type Clerk struct {
	servers []*labrpc.ClientEnd // server的数目
	// You will have to modify this struct.
	Client_id  int64
	Request_id int
	Leader_id  int
}

func nrand() int64 { // 创建clerk时，赋予其id（生产环境下会用IP:PORT的方法去定位具体的Client，或者clerk）
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.Client_id = nrand()
	ck.Leader_id = Get_Random_Server(len(ck.servers))
	// ck.Request_id = 0

	return ck
}

func Get_Random_Server(length int) int {
	return mathrand.Intn(length)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.Request_id += 1
	leader_id := ck.Leader_id // 任意的server都行（任意副本都能返回Get），但为了一致性，必须call leader

	for {
		args := GetArgs{
			Key:        key,
			Client_id:  ck.Client_id,
			Request_id: ck.Request_id,
		}
		reply := GetReply{}

		ok := ck.servers[leader_id].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader { // 如果出现问题，则切换询问对象
			leader_id = (leader_id + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrNoKey { // 没有key
			return ""
		}
		if reply.Err == OK { // 获得value
			ck.Leader_id = leader_id
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.Request_id += 1
	leader_id := ck.Leader_id // 任意的server都行（任意副本都能返回Get）

	for {
		args := PutAppendArgs{
			Key:        key,
			Value:      value,
			Op:         op,
			Client_id:  ck.Client_id,
			Request_id: ck.Request_id,
		}
		reply := PutAppendReply{}

		ok := ck.servers[leader_id].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader { // 如果出现问题，则切换询问对象
			leader_id = (leader_id + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK { // 获得value
			ck.Leader_id = leader_id
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
