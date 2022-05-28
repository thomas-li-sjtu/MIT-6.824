package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getRand(server int64) int {
	rand.Seed(time.Now().Unix() + server)
	return rand.Intn(100-50) + 50
}

// 获得最后一个log的index
func (rf *Raft) get_last_index() int {
	return len(rf.log) - 1 + rf.last_snapshot_point_index
}

// 获得最后一个log对应的term  注意，最开始log里有一个空的entry
func (rf *Raft) get_last_term() int {
	if len(rf.log)-1 == 0 {
		return rf.last_snapshot_point_term // 当前没有log记录，因此返回上一个快照log的记录
	} else {
		return rf.log[len(rf.log)-1].Term // 当前有log，直接读取最后一个log的term
	}
}

// leader获得某个raft实例的Prev_log_index、term
func (rf *Raft) get_prev_log_info(follower_id int) (int, int) {
	prev_log_index := rf.next_index[follower_id] - 1
	last_index := rf.get_last_index()
	if prev_log_index == last_index+1 {
		prev_log_index = last_index
	}
	return prev_log_index, rf.get_log_term(prev_log_index)
}

// raft通过log index获取相应log的term
func (rf *Raft) get_log_term(log_index int) int {
	if log_index == rf.last_snapshot_point_index {
		return rf.last_snapshot_point_term
	}
	return rf.log[log_index-rf.last_snapshot_point_index].Term
}

// raft通过log index获得相应的log entry
func (rf *Raft) get_entry(log_index int) Entry {
	return rf.log[log_index-rf.last_snapshot_point_index]
}

// 检查输入的index和term是否新于当前raft存储的log
func (rf *Raft) need_update(index, term int) bool {
	cur_log_index_saved := rf.get_last_index()
	cur_term_saved := rf.get_last_term()
	return term > cur_term_saved ||
		(cur_term_saved == term && index >= cur_log_index_saved)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}
