package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Heat_beat_time = 50 * time.Millisecond // leader广播心跳的时间间隔
	Apply_time     = 51 * time.Millisecond
	TO_FOLLOWER    = 0
	TO_CANDIDATE   = 1
	TO_LEADER      = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	voted_for    int       // candidate id（rf.me）that received vote in current term
	cur_term     int       // 当前的term
	timeout      time.Time // 接收心跳的时间上线，超过该时间则follower成为candidate
	state        string    // 状态: follower candidate leader
	get_vote_num int       // 该实例得到的票数

	log []Entry

	commit_index int // index of highest log entry known to be committed
	last_applied int // index of highest log entry applied to state machine

	next_index  []int // 对于第i个服务器，要发送给该服务器的下一个log entry的索引。
	match_index []int // 对于第i个服务器，已知的在该服务器上复制的最高log entry的索引

	applyCh chan ApplyMsg
	// SnapShot Point use
	last_snapshot_point_index int
	last_snapshot_point_term  int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type Entry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.cur_term, rf.state == "leader"
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var persist_currentTrem int
	var persist_voteFor int
	var persist_log []Entry
	//var persist_lastApplied int
	var persist_lastSSPointIndex int
	var persist_lastSSPointTerm int
	//var persist_snapshot []byte

	if d.Decode(&persist_currentTrem) != nil ||
		d.Decode(&persist_voteFor) != nil ||
		d.Decode(&persist_log) != nil ||
		//d.Decode(&persist_lastApplied) != nil ||
		d.Decode(&persist_lastSSPointIndex) != nil ||
		d.Decode(&persist_lastSSPointTerm) != nil {
		//d.Decode(&persist_snapshot) != nil{
		DPrintf("%d read persister got a problem!!!!!!!!!!", rf.me)
	} else {
		rf.cur_term = persist_currentTrem
		rf.voted_for = persist_voteFor
		rf.log = persist_log
		// rf.lastApplied = persist_lastApplied
		rf.last_snapshot_point_index = persist_lastSSPointIndex
		rf.last_snapshot_point_term = persist_lastSSPointTerm
		// rf.persister.SaveStateAndSnapshot(rf.persistData(),persist_snapshot)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) persistData() []byte {

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.cur_term)
	e.Encode(rf.voted_for)
	e.Encode(rf.log)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.last_snapshot_point_index)
	e.Encode(rf.last_snapshot_point_term)
	//e.Encode(rf.persister.ReadSnapshot())
	data := w.Bytes()
	return data
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1, -1, false
	}
	if rf.state != "leader" {
		return -1, -1, false
	} else {
		// leader raft为该命令建立一个log entry
		index := rf.get_last_index() + 1
		rf.log = append(rf.log, Entry{Term: rf.cur_term, Command: command})
		// rf.persist()
		return index, rf.cur_term, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.voted_for = -1
	rf.cur_term = 0
	rf.state = "follower" // 初始所有rf节点为follower
	rf.get_vote_num = 0
	rf.commit_index = 0
	rf.last_applied = 0
	rf.log = []Entry{}
	rf.log = append(rf.log, Entry{})
	rf.last_snapshot_point_index = 0
	rf.last_snapshot_point_term = 0
	rf.applyCh = applyCh
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.last_snapshot_point_index > 0 {
		rf.last_applied = rf.last_snapshot_point_index
	}

	// start ticker goroutine to start elections
	go rf.candidate_election_ticker()
	// start ticker goroutine to append entries (including sending heartbeat)
	go rf.leader_append_entries_ticker()
	// 模拟集群把已经commited 的Entry应用在状态虚拟机上，检测是否存在已经被commited但没有Applied的Entry，将Entry信息发送给测试系统提供的chan中
	go rf.committed_to_apply_ticker()

	return rf
}
