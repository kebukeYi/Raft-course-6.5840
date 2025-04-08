package raft

import (
	"fmt"
	"time"
)

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

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success,
		reply.ConflictIndex, reply.ConflictTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String()) // replay initialized
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
	if args.PrevLogIndex >= rf.rlog.size() {
		reply.Term = InvalidTerm
		reply.ConflictIndex = rf.rlog.size()
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d",
			args.LeaderId, rf.rlog.size(), args.PrevLogIndex)
		return
	}

	// 出现 follower 私自截断日志, leader需要重新下发日志;
	if rf.rlog.snapLastIndex > args.PrevLogIndex {
		reply.ConflictTerm = rf.rlog.snapLastTerm
		reply.ConflictIndex = rf.rlog.snapLastIndex
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d",
			args.LeaderId, rf.rlog.snapLastIndex)
		return
	}

	// follower 相同位置, 但是任期不同;
	// leader 收到结果后,进行判断:
	// - leader没有这个任期的日志, 根据传回的 ConflictIndex-1 进一步计算;
	// - Leader日志中存在 ConfilictTerm 的日志, 则使用其任期内在Leader的首条日志位置,
	//   重新向follower发送日志判断, 然后就将冲突任期的日志重新覆盖一次;
	if rf.rlog.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.rlog.at(args.PrevLogIndex).Term
		reply.ConflictIndex = rf.rlog.firstForTerm(reply.ConflictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d",
			args.LeaderId, args.PrevLogIndex, rf.rlog.at(args.PrevLogIndex).Term, args.PrevLogTerm)
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
		if logIdx < rf.rlog.size() && rf.rlog.at(logIdx).Term != entry.Term {
			LOG(rf.me, rf.currentTerm, DLog2, "Follower truncate logsIndex: %d", logIdx)
			rf.rlog.truncate(logIdx)
		}
		if logIdx >= rf.rlog.size() {
			LOG(rf.me, rf.currentTerm, DLog2, "Follower append logsIndex: %d", logIdx)
			rf.rlog.append(entry)
		}
		// 说明 当前 entry 已经在 follower日志存在了,继续下一个entry;
	}
	reply.Success = true
	rf.persistLocked()
	// TODO: handle the args.LeaderCommit
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = min(args.LeaderCommit, rf.rlog.size()-1)
		rf.applyCond.Signal()
	}
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

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())
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
				findFirstLogIndexAtTerm := rf.rlog.firstForTerm(reply.ConflictTerm)
				if findFirstLogIndexAtTerm == InvalidIndex {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					rf.nextIndex[peer] = findFirstLogIndexAtTerm
				}
			}

			// avoid the late reply move the nextIndex forward again
			rf.nextIndex[peer] = min(preNext, rf.nextIndex[peer])

			nextPrevIndex := rf.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm
			if nextPrevIndex >= rf.rlog.snapLastIndex {
				nextPrevTerm = rf.rlog.at(nextPrevIndex).Term
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, rf.rlog.String())
			return
		}

		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO: need compute the new commitIndex here
		majorityIndexLocked := rf.getMajorityIndexLocked()
		if majorityIndexLocked > rf.commitIndex && rf.rlog.at(majorityIndexLocked).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d",
				rf.commitIndex, majorityIndexLocked)
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
			rf.matchIndex[i] = rf.rlog.size() - 1
			rf.nextIndex[i] = rf.rlog.size()
			continue
		}

		// 默认rf.nextIndex[i]是len(rf.log),因此头一次下发日志后,会进行日志回退检测;
		prevIndex := rf.nextIndex[i] - 1

		// leader要下发日志, 发现其日志被自身截断了,那么就下发快照吧;
		if rf.rlog.snapLastIndex > prevIndex {
			args := &InstallSnapshotArgs{
				LeaderId:      rf.me,
				Term:          rf.currentTerm,
				LastSnapIndex: rf.rlog.snapLastIndex,
				LastSnapTerm:  rf.rlog.snapLastTerm,
				SnapShot:      rf.rlog.snapshot,
			}
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", i, args.String())
			go rf.sendToPeerSnapshot(i, term, args)
			continue
		}

		prevTerm := rf.rlog.at(prevIndex).Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      append([]LogEntry(nil), rf.rlog.tail(prevIndex+1)...),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Send log, Prev=[%d]T%d, Len()=%d", i, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
		go replicateToPeer(i, args)
	}
	return true
}
