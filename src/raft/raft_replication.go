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
	Term    int
	Success bool
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

	// 0 1 2 3 4 len=5
	//   1 2 3 4 5 6
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d",
			args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d",
			args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}

	// 正好匹配,直接可逐个添加;
	// L:    [p--------------]
	// F: [---e]

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

	// TODO: handle the args.LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Signal()
	}
	rf.reSetElectionBeat()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
			idx := rf.nextIndex[peer] - 1
			term := rf.log[idx].Term
			// todo leader 进行日志回退检测;
			for idx > 0 && rf.log[idx].Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO: need compute the new commitIndex here
		majorityIndexLocked := rf.getMajorityIndexLocked()
		if majorityIndexLocked > rf.commitIndex {
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
