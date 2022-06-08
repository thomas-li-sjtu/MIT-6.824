package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	Client_id  int64
	Request_id int
	Leader_id  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.Client_id = nrand()
	ck.Leader_id = Get_Random_Server(len(ck.servers))
	return ck
}

func Get_Random_Server(length int) int {
	return mathrand.Intn(length)
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.Request_id += 1
	args := &QueryArgs{
		Num:        num,
		Client_id:  ck.Client_id,
		Request_id: ck.Request_id,
	}
	server := ck.Leader_id
	for {
		// try each known server.
		reply := QueryReply{}
		ok := ck.servers[server].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.Leader_id = server
			return reply.Config
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	// Your code here.
	ck.Request_id += 1
	args := &JoinArgs{
		Servers:    servers,
		Client_id:  ck.Client_id,
		Request_id: ck.Request_id,
	}
	server := ck.Leader_id
	for {
		// try each known server.
		reply := JoinReply{}
		ok := ck.servers[server].Call("ShardCtrler.Join", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.Leader_id = server
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.Request_id += 1
	args := &LeaveArgs{
		GIDs:       gids,
		Client_id:  ck.Client_id,
		Request_id: ck.Request_id,
	}
	server := ck.Leader_id

	for {
		// try each known server.
		reply := LeaveReply{}
		ok := ck.servers[server].Call("ShardCtrler.Leave", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.Leader_id = server
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.Request_id += 1
	args := &MoveArgs{
		Shard:      shard,
		GID:        gid,
		Client_id:  ck.Client_id,
		Request_id: ck.Request_id,
	}
	server := ck.Leader_id

	for {
		// try each known server.
		reply := MoveReply{}
		ok := ck.servers[server].Call("ShardCtrler.Move", args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			ck.Leader_id = server
			return
		}

		time.Sleep(100 * time.Millisecond)
	}
}
