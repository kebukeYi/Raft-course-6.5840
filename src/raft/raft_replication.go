package raft

import "time"

type LogEntry struct {
	CommandValid bool
	Command      interface{}
	Term         int
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}

type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// For debug
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Receive log, Prev=[%d]T%d, Len()=%d", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	// replay initialized
	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}
	rf.reSetElectionBeat()

	// 0 1 2 3 4 len=5
	//   1 2 3 4 5 6
	// follower 日志太短, 需要直接添加;
	if args.PrevLogIndex >= len(rf.log) {
		reply.Term = InvalidTerm
		reply.ConflictIndex = len(rf.log)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	// follower 相同位置, 但是任期不同;
	// leader 收到结果后,进行判断:
	// - leader没有这个任期的日志, 根据传回的 ConflictIndex-1 进一步计算;
	// - Leader日志中存在 ConfilictTerm 的日志, 则使用其任期内在Leader的首条日志位置,
	//   重新向follower发送日志判断, 然后就将冲突任期的日志重新覆盖一次;
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		reply.ConflictIndex = rf.findFirstLogIndexAtTerm(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 正好匹配,直接可逐个添加;
	// L:    [p--------------]
	// F: [---e]

	// index匹配,但是term不匹配;
	// L:    [p--------------]
	// F: [---q]

	// follower 缺失日志, 返回失败, leader进行日志回退;
	// L:    [--------------]
	// F: [-]

	// follower 多了一点日志;
	// L:   [p --------------]
	// F:[---e --]

	// follower 多了很多日志;
	// L:   [p --------------]
	// F: [--e -----------------]
	for i, entry := range args.Entries {
		logIdx := args.PrevLogIndex + 1 + i
		if logIdx < len(rf.log) && rf.log[logIdx].Term != entry.Term {
			LOG(rf.me, rf.currentTerm, DLog2, "Follower truncate logsIndex: %d", logIdx)
			rf.log = rf.log[:logIdx]
		}
		if logIdx >= len(rf.log) {
			LOG(rf.me, rf.currentTerm, DLog2, "Follower append logsIndex: %d", logIdx)
			rf.log = append(rf.log, entry)
		}
		// 说明 当前 entry 已经在 follower日志存在了,继续下一个entry;
	}
	reply.Success = true
	rf.persistLocked()
	// TODO: handle the args.LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) findFirstLogIndexAtTerm(term int) int {
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex

}

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		if !reply.Success {
			preNext := rf.nextIndex[peer]

			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				findFirstLogIndexAtTerm := rf.findFirstLogIndexAtTerm(reply.ConflictTerm)
				if findFirstLogIndexAtTerm == InvalidIndex {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					rf.nextIndex[peer] = findFirstLogIndexAtTerm
				}
			}

			// avoid the late reply move the nextIndex forward again
			rf.nextIndex[peer] = min(preNext, rf.nextIndex[peer])
			LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO: need compute the new commitIndex here
		majorityIndexLocked := rf.getMajorityIndexLocked()
		if majorityIndexLocked > rf.commitIndex && rf.log[majorityIndexLocked].Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityIndexLocked)
			rf.commitIndex = majorityIndexLocked
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.matchIndex[i] = len(rf.log) - 1
			rf.nextIndex[i] = len(rf.log)
			continue
		}

		// 默认rf.nextIndex[i]是len(rf.log),因此头一次下发日志后,会进行日志回退检测;
		prevIndex := rf.nextIndex[i] - 1
		prevTerm := rf.log[prevIndex].Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      append([]LogEntry(nil), rf.log[prevIndex+1:]...),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send log, Prev=[%d]T%d, Len()=%d",
			i, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
		go replicateToPeer(i, args)
	}
	return true
}
