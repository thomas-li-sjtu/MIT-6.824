package raft

import (
	"time"
)

type AppendEntryArgs struct {
	Term           int // 当前leader任期
	Leader_id      int // 当前leader id
	Prev_log_index int
	Prev_log_term  int
	Entries        []Entry // empty for hearbeat, send a list for efficiency
	Leader_commit  int     // index of leader's commit index
}

type AppendEntryReply struct {
	Term    int  // current term
	Success bool // True if follower contained entry which matched the prev_log_index and term
	// TODO: ConflictingIndex
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.cur_term > args.Term {
		// 不满足rule1
		reply.Term = rf.cur_term
		reply.Success = false
		return
	}

	rf.cur_term = args.Term
	reply.Term = rf.cur_term
	reply.Success = true

	// 收到心跳，更新状态和时间戳
	if rf.state != "follower" {
		rf.change_state(TO_FOLLOWER, true)
	} else {
		rf.timeout = time.Now()
	}
	return
}

func (rf *Raft) leader_append_entries() {
	for index := range rf.peers {
		go func(follower_id int) {
			if follower_id == rf.me { // 不向自己发送消息
				return
			}

			rf.mu.Lock()
			if rf.state != "leader" {
				rf.mu.Unlock()
				return
			}

			entry_args := AppendEntryArgs{}
			if rf.get_last_index() >= rf.next_index[follower_id] {
				// 发送append entry RPC
				// TODO: 一般的entry
				entry_args = AppendEntryArgs{
					rf.cur_term,
					rf.me,
					0, // prevLogIndex,
					0, // prevLogTerm,
					[]Entry{},
					rf.commit_index,
				}
			} else {
				// 发送心跳
				// TODO: 初始化prevLogIndex和prevLogTerm
				// prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
				entry_args = AppendEntryArgs{
					rf.cur_term,
					rf.me,
					0, // prevLogIndex,
					0, // prevLogTerm,
					[]Entry{},
					rf.commit_index,
				}
			}
			entry_reply := AppendEntryReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(follower_id, &entry_args, &entry_reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if rf.state != "leader" {
					// 可能不再是leader
					return
				} else {
					if rf.cur_term < entry_reply.Term {
						// 可能任期不足，此时rf
						rf.cur_term = entry_reply.Term
						rf.change_state(TO_FOLLOWER, true)
					} else {
						return
						// if entry_reply.Success { // 成功
						// 	rf.mu.Unlock()
						// } else { // 失败
						// 	rf.mu.Unlock()
						// }
					}
				}
			}
		}(index)
	}
}

func (rf *Raft) leader_append_entries_ticker() {
	for !rf.killed() {
		time.Sleep(heat_beat_time)
		rf.mu.Lock()
		if rf.state == "leader" {
			rf.mu.Unlock()
			rf.leader_append_entries()
		} else {
			rf.mu.Unlock()
		}
	}
}
