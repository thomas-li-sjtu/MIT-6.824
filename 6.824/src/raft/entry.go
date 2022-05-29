package raft

import (
	"time"
)

type AppendEntryArgs struct {
	Term           int // 当前leader任期
	Leader_id      int // 当前leader id
	Prev_log_index int // 新entry之前的log entry的索引
	Prev_log_term  int
	Entries        []Entry // empty for hearbeat, send a list for efficiency
	Leader_commit  int     // index of leader's commit index
}

type AppendEntryReply struct {
	Term             int  // current term
	Success          bool // True if follower contained entry which matched the prev_log_index and term
	ConflictingIndex int  //帮助修正leader的next_index[]（如果Success为False）
}

func (rf *Raft) update_commit(is_leader bool, commit_index int) {
	if !is_leader {
		// AppendEntries rule5
		if commit_index > rf.commit_index {
			index_last_entry := rf.get_last_index()
			if commit_index >= index_last_entry {
				rf.commit_index = index_last_entry
			} else {
				rf.commit_index = commit_index
			}
		}
	} else {
		// Rules for Servers, Leaders rule4，更新leader的commit index
		rf.commit_index = rf.last_snapshot_point_index
		for index := rf.get_last_index(); index >= rf.last_snapshot_point_index+1; index-- {
			// N的范围：[last_snapshot_point_index+1, get_last_index()]
			sum := 0
			for i := range rf.peers {
				if i == rf.me {
					sum += 1
				} else {
					if rf.match_index[i] >= index {
						sum += 1
					}
				}
			}

			if sum >= len(rf.peers)/2+1 && rf.get_log_term(index) == rf.cur_term {
				rf.commit_index = index
				break
			}
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.cur_term > args.Term {
		// 不满足rule1
		reply.Term = rf.cur_term
		reply.Success = false
		reply.ConflictingIndex = -1
		return
	}

	rf.cur_term = args.Term
	reply.Term = rf.cur_term
	reply.Success = true
	reply.ConflictingIndex = -1
	// 收到心跳，更新状态和时间戳
	if rf.state != "follower" {
		rf.change_state(TO_FOLLOWER, true)
	} else {
		rf.timeout = time.Now()
	}
	rf.persist() // 此时cur_term可能发生了改变（不经过change state），因此需要persist

	// TODO: 考虑snapshot
	if rf.last_snapshot_point_index > args.Prev_log_index {
		reply.Success = false
		reply.ConflictingIndex = rf.get_last_index() + 1
		return
	}

	if rf.get_last_index() < args.Prev_log_index {
		// 不满足rule2
		reply.Success = false
		reply.ConflictingIndex = rf.get_last_index()
		// fmt.Println("rule2 failed")
		return
	} else {
		if rf.get_log_term(args.Prev_log_index) != args.Prev_log_term {
			reply.Success = false
			temp_term := rf.get_log_term(args.Prev_log_index)
			for index := args.Prev_log_index; index >= rf.last_snapshot_point_index; index-- {
				if rf.get_log_term(index) != temp_term {
					reply.ConflictingIndex = index + 1
					break
				}
			}
			// fmt.Println("rule2 failed")
			return
		}
	}

	// rule3、rule4
	rf.log = append(rf.log[:args.Prev_log_index-rf.last_snapshot_point_index+1], args.Entries...) // ...表明args.Entries需要解包
	rf.persist()

	// rule5
	if args.Leader_commit > rf.commit_index {
		rf.update_commit(false, args.Leader_commit)
	}
}

func (rf *Raft) leader_append_entries() {
	for index := range rf.peers {
		if index == rf.me { // 不向自己发送消息
			continue
		}

		go func(follower_id int) {
			rf.mu.Lock()
			if rf.state != "leader" {
				rf.mu.Unlock()
				return
			}
			// TODO: 缺少对snapshot的处理
			prevLogIndextemp := rf.next_index[follower_id] - 1
			// DPrintf("[IfNeedSendSnapShot] leader %d ,lastSSPIndex %d, server %d ,prevIndex %d",rf.me,rf.lastSSPointIndex,server,prevLogIndextemp)
			if prevLogIndextemp < rf.last_snapshot_point_index {
				go rf.leaderSendSnapShot(follower_id)
				rf.mu.Unlock()
				return
			}

			entry_args := AppendEntryArgs{}
			if rf.get_last_index() >= rf.next_index[follower_id] {
				// 发送append entry RPC
				entry_to_replicate := make([]Entry, 0)
				entry_to_replicate = append(entry_to_replicate, rf.log[rf.next_index[follower_id]-rf.last_snapshot_point_index:]...)
				prev_log_index, prev_log_term := rf.get_prev_log_info(follower_id)
				entry_args = AppendEntryArgs{
					Term:           rf.cur_term,
					Leader_id:      rf.me,
					Prev_log_index: prev_log_index,
					Prev_log_term:  prev_log_term,
					Entries:        entry_to_replicate,
					Leader_commit:  rf.commit_index,
				}
			} else {
				// 发送心跳
				prev_log_index, prev_log_term := rf.get_prev_log_info(follower_id)
				entry_args = AppendEntryArgs{
					Term:           rf.cur_term,
					Leader_id:      rf.me,
					Prev_log_index: prev_log_index, // prevLogIndex,
					Prev_log_term:  prev_log_term,  // prevLogTerm,
					Entries:        []Entry{},
					Leader_commit:  rf.commit_index,
				}
			}
			entry_reply := AppendEntryReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(follower_id, &entry_args, &entry_reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != "leader" {
					// 可能不再是leader
					return
				}

				if rf.cur_term < entry_reply.Term {
					// 可能任期不足
					rf.cur_term = entry_reply.Term
					rf.change_state(TO_FOLLOWER, true) // change_state中persist
					return
				}

				// Rules for servers,leaders rule3
				if entry_reply.Success { // 成功
					rf.match_index[follower_id] = len(entry_args.Entries) + entry_args.Prev_log_index
					rf.next_index[follower_id] = rf.match_index[follower_id] + 1
					rf.update_commit(true, -1)
					// if len(entry_args.Entries) != 0 {
					// 	fmt.Println("log index", rf.get_last_index(), "replicated in follower", follower_id)
					// }
				} else { // 失败
					if entry_reply.ConflictingIndex != -1 {
						// fmt.Println("something wrong")
						rf.next_index[follower_id] = entry_reply.ConflictingIndex
					}
				}
			}
		}(index)
	}
}

// start ticker goroutine to append entries (including sending heartbeat)
func (rf *Raft) leader_append_entries_ticker() {
	for !rf.killed() {
		time.Sleep(Heat_beat_time)
		rf.mu.Lock()
		if rf.state == "leader" {
			rf.mu.Unlock()
			rf.leader_append_entries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// 模拟集群把已经commited 的Entry应用在状态虚拟机上，检测是否存在已经被commited但没有Applied的Entry，将Entry信息发送给测试系统提供的chan中
func (rf *Raft) committed_to_apply_ticker() {
	for !rf.killed() {
		time.Sleep(Apply_time)
		rf.mu.Lock()

		if rf.last_applied >= rf.commit_index {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.last_applied < rf.commit_index && rf.last_applied < rf.get_last_index() {
			rf.last_applied += 1 // index都从1开始，last_applied初始化为0
			Messages = append(
				Messages, ApplyMsg{
					CommandValid:  true,
					Command:       rf.get_entry(rf.last_applied).Command,
					CommandIndex:  rf.last_applied,
					SnapshotValid: false,
				})
		}
		rf.mu.Unlock() // 必须先unlock，再发给applyCh
		// fmt.Println(rf.me, rf.state, Messages)
		for _, message := range Messages {
			rf.applyCh <- message
		}
	}
}
