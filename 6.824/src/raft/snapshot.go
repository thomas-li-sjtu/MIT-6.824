package raft

import "time"

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.last_snapshot_point_index >= index || index > rf.commit_index {
		return
	}
	// snapshot the entrier form 1:index(global)
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.get_last_index(); i++ {
		tempLog = append(tempLog, rf.get_entry(i))
	}

	// TODO fix it in lab 4
	if index == rf.get_last_index()+1 {
		rf.last_snapshot_point_term = rf.get_last_term()
	} else {
		rf.last_snapshot_point_term = rf.get_log_term(index)
	}

	rf.last_snapshot_point_index = index

	rf.log = tempLog
	if index > rf.commit_index {
		rf.commit_index = index
	}
	if index > rf.commit_index {
		rf.last_applied = index
	}
	DPrintf("[SnapShot]Server %d sanpshot until index %d, term %d, loglen %d", rf.me, index, rf.last_snapshot_point_term, len(rf.log)-1)
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// DPrintf("[lock] sever %d get the lock",rf.me)
	if rf.cur_term > args.Term {
		reply.Term = rf.cur_term
		rf.mu.Unlock()
		return
	}

	rf.cur_term = args.Term
	reply.Term = args.Term
	if rf.state != "follower" {
		rf.change_state(TO_FOLLOWER, true)
	} else {
		rf.timeout = time.Now()
		rf.persist()
	}

	if rf.last_snapshot_point_index >= args.LastIncludeIndex {
		DPrintf("[HaveSnapShot] sever %d , lastSSPindex %d, leader's lastIncludeIndex %d", rf.me, rf.last_snapshot_point_index, args.LastIncludeIndex)
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludeIndex
	tempLog := make([]Entry, 0)
	tempLog = append(tempLog, Entry{})

	for i := index + 1; i <= rf.get_last_index(); i++ {
		tempLog = append(tempLog, rf.get_entry(i))
	}

	rf.last_snapshot_point_term = args.LastIncludeTerm
	rf.last_snapshot_point_index = args.LastIncludeIndex

	rf.log = tempLog
	if index > rf.commit_index {
		rf.commit_index = index
	}
	if index > rf.last_applied {
		rf.last_applied = index
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	//rf.persist()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.last_snapshot_point_term,
		SnapshotIndex: rf.last_snapshot_point_index,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
	DPrintf("[FollowerInstallSnapShot]server %d installsnapshot from leader %d, index %d", rf.me, args.LeaderId, args.LastIncludeIndex)

}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	DPrintf("[LeaderSendSnapShot]Leader %d (term %d) send snapshot to server %d, index %d", rf.me, rf.cur_term, server, rf.last_snapshot_point_index)
	ssArgs := InstallSnapshotArgs{
		rf.cur_term,
		rf.me,
		rf.last_snapshot_point_index,
		rf.last_snapshot_point_term,
		rf.persister.ReadSnapshot(),
	}
	ssReply := InstallSnapshotReply{}
	rf.mu.Unlock()

	re := rf.sendSnapShot(server, &ssArgs, &ssReply)

	if !re {
		DPrintf("[InstallSnapShot ERROR] Leader %d don't recive from %d", rf.me, server)
	}
	if re == true {
		rf.mu.Lock()
		if rf.state != "leader" || rf.cur_term != ssArgs.Term {
			rf.mu.Unlock()
			return
		}
		if ssReply.Term > rf.cur_term {
			rf.change_state(TO_FOLLOWER, true)
			rf.mu.Unlock()
			return
		}

		DPrintf("[InstallSnapShot SUCCESS] Leader %d from sever %d", rf.me, server)
		rf.match_index[server] = ssArgs.LastIncludeIndex
		rf.next_index[server] = ssArgs.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}
