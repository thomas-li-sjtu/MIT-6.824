package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term            int // 申请选举的任期
	Candidate_index int // candidate的id
	Last_log_term   int // term of candidate's last log entry
	Last_log_index  int // index of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int  // 投票者的任期
	Vote_for_you bool // 是否投票给args对应的candidate
}

func (rf *Raft) change_state(change_to int, reset_timestamp bool) {
	if change_to == TO_CANDIDATE {
		rf.state = "candidate"
		rf.cur_term += 1
		rf.get_vote_num = 1 // 投自己一票
		rf.voted_for = rf.me
		rf.persist()
		rf.candidate_election()
		rf.timeout = time.Now()
	}

	if change_to == TO_FOLLOWER {
		rf.state = "follower"
		rf.get_vote_num = 0
		rf.voted_for = -1
		rf.persist()
		if reset_timestamp {
			rf.timeout = time.Now()
		}
	}

	if change_to == TO_LEADER {
		rf.state = "leader"
		rf.voted_for = -1
		rf.get_vote_num = 0
		rf.persist()

		rf.next_index = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ { // leader始终是最新的，next_index表明接下来要给follower发送的log index
			rf.next_index[i] = rf.get_last_index() + 1
		}
		rf.match_index = make([]int, len(rf.peers))
		rf.match_index[rf.me] = rf.get_last_index() // match_index表明，某一follower已经存储到的log index

		rf.timeout = time.Now()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// rf为接收申请的服务器
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.cur_term > args.Term {
		// candidate的term偏小（fig2 rule1）
		// fmt.Println("Election reject ----- candidate term: " + strconv.Itoa(args.Term) + "smaller than current raft term")
		reply.Vote_for_you = false
		reply.Term = rf.cur_term
		return
	}

	// candidate的term不小于rf的term时，考虑两种情况（fig2 rule2）
	reply.Term = rf.cur_term
	if rf.cur_term < args.Term {
		// rf的term偏小，rf转为follower
		// fmt.Println(rf.me, "change to follower")
		rf.cur_term = args.Term
		rf.change_state(TO_FOLLOWER, false)
		rf.persist()
	}

	if !rf.need_update(args.Last_log_index, args.Last_log_term) {
		// 不满足rule2
		reply.Vote_for_you = false
		reply.Term = rf.cur_term
	} else {
		if rf.voted_for != -1 && rf.voted_for != args.Candidate_index &&
			rf.cur_term == args.Term {
			// 可能已经给其他人投票了（或者自己就是candidate）
			// fmt.Println(rf.me, "vote for others")
			reply.Vote_for_you = false
			reply.Term = rf.cur_term
		} else {
			rf.voted_for = args.Candidate_index
			rf.cur_term = args.Term
			rf.timeout = time.Now() // 重置时间
			reply.Vote_for_you = true
			reply.Term = rf.cur_term
			rf.persist()
		}
	}
}

func (rf *Raft) candidate_election() {
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// 并行发送request vote和处理request reply
		go func(server_id int) {
			rf.mu.Lock()
			vote_args := RequestVoteArgs{
				Term:            rf.cur_term,
				Candidate_index: rf.me,
				Last_log_term:   rf.get_last_term(),
				Last_log_index:  rf.get_last_index(),
			}
			vote_reply := RequestVoteReply{}
			rf.mu.Unlock()

			ok := rf.sendRequestVote(server_id, &vote_args, &vote_reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if rf.state != "candidate" || vote_args.Term < rf.cur_term {
					// 发送request前后，rf的term发生改变
					return
				}
				if vote_reply.Term > vote_args.Term {
					// rule1不满足
					if rf.cur_term < vote_reply.Term {
						rf.cur_term = vote_reply.Term
					}
					rf.change_state(TO_FOLLOWER, false) // 没有收到appendentry rpc时，不要重置时间
					return
				}
				if vote_args.Term == rf.cur_term && vote_reply.Vote_for_you == true {
					// 记录投票
					rf.get_vote_num += 1
					if rf.get_vote_num >= len(rf.peers)/2+1 {
						// fmt.Println(rf.me, "win the election")
						rf.change_state(TO_LEADER, true)
						return
					}
					return
				}
			}
		}(index)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) candidate_election_ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using time.Sleep().
		now := time.Now()
		time.Sleep(time.Duration(getRand(int64(rf.me))) * time.Millisecond) // 随机选举超时时间，错开时间以防止选票分割
		// 这里随机的时间长于心跳，因此如果leader出问题，那么下面的if判断一定为true
		// 而如果leader没有出问题，那么rf.timeout会在另一个goroutine里被更新，下面if判断一定为false
		rf.mu.Lock()
		if rf.timeout.Before(now) && rf.state != "leader" {
			// 当前时间在rf心跳期限之后，并且rf不是leader
			rf.change_state(TO_CANDIDATE, true)
		}
		rf.mu.Unlock()
	}
}
