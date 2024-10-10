package raft

import (
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool
	Command      interface{}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

func (raft *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	raft.mux.Lock()
	defer raft.mux.Unlock()
	Log(raft.me, raft.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	reply.Term = raft.currentTerm
	reply.Success = false

	if args.Term < raft.currentTerm {
		Log(raft.me, raft.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, raft.currentTerm)
		return
	}

	if args.Term >= raft.currentTerm {
		raft.becomeFollowerLocked(args.Term)
	}

	defer func() {
		raft.resetElectionTimerLocked()
		if !reply.Success {
			Log(raft.me, raft.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			Log(raft.me, raft.currentTerm, DDebug, "<- S%d, Follower Log=%v", args.LeaderId, raft.log.String())
		}
	}()

	if args.PrevLogIndex >= raft.log.size() {
		//reply.Term = InvalidTerm // error
		reply.ConflictTerm = InvalidTerm
		reply.ConflictIndex = raft.log.size()
		Log(raft.me, raft.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderId, raft.log.size(), args.PrevLogIndex)
		return
	}

	// 快照任期 快照索引
	// 这种情况会发生吗?
	// 既然follower节点的快照索引有值,那么说明之前leader下发过快照;
	// 前提leader下发快照, 那么就说明这部分数据已经被大多数节点提交了; 也有可能此节点没有在大多数节点之中;
	if raft.log.snapshotIndex > args.PrevLogIndex {
		reply.ConflictIndex = raft.log.snapshotIndex
		reply.ConflictTerm = raft.log.snapshotTerm
		Log(raft.me, raft.currentTerm, DLog2, "<- S%d, Reject log, Follower log truncated in %d", args.LeaderId, raft.log.snapshotIndex)
		return
	}

	// 冲突任期 冲突首索引
	if raft.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = raft.log.at(args.PrevLogIndex).Term
		reply.ConflictIndex = raft.log.firstFor(reply.ConflictTerm)
		Log(raft.me, raft.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, raft.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	raft.log.appendFrom(args.PrevLogIndex, args.Entries)
	raft.persistLocked()
	reply.Success = true
	Log(raft.me, raft.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	// 正常下发日志, 更近提交进度
	if args.LeaderCommit > raft.commitIndex {
		Log(raft.me, raft.currentTerm, DApply, "Follower update the commit index %d->%d", raft.commitIndex, args.LeaderCommit)
		raft.commitIndex = args.LeaderCommit
		raft.applyCond.Signal()
	}
}

func (raft *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := raft.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (raft *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := raft.sendAppendEntries(peer, args, reply)

		raft.mux.Lock()
		defer raft.mux.Unlock()
		if !ok {
			Log(raft.me, raft.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		Log(raft.me, raft.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		if reply.Term > raft.currentTerm {
			raft.becomeFollowerLocked(reply.Term)
			return
		}

		if raft.contextLockIsLost(leader, term) {
			Log(raft.me, raft.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, raft.currentTerm, raft.role)
			return
		}

		// handle the reply
		// probe the lower index if the prevLog not matched
		if !reply.Success {
			prevIndex := raft.nextIndex[peer]
			// 说明 日志相差太多 或者 第一次发送日志, 就从落后方开始;
			if reply.ConflictTerm == InvalidTerm {
				raft.nextIndex[peer] = reply.ConflictIndex
			} else {
				// 否则 从冲突的term的第一个日志开始
				firstForIndex := raft.log.firstFor(reply.ConflictTerm)
				// 找到该term的第一个日志
				if firstForIndex != InvalidIndex {
					raft.nextIndex[peer] = firstForIndex
				} else {
					// 未找到时，使用对端提供的冲突索引作为备选
					// 什么情况下 没有找到呢?
					raft.nextIndex[peer] = reply.ConflictIndex
				}
			}

			if raft.nextIndex[peer] > prevIndex {
				raft.nextIndex[peer] = prevIndex
			}

			nextPrevIndex := raft.nextIndex[peer] - 1
			nextPrevTerm := InvalidTerm

			if nextPrevIndex >= raft.log.snapshotIndex {
				nextPrevTerm = raft.log.at(nextPrevIndex).Term
			}

			Log(raft.me, raft.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d",
				peer, args.PrevLogIndex, args.PrevLogTerm, nextPrevIndex, nextPrevTerm)
			Log(raft.me, raft.currentTerm, DDebug, "-> S%d, Leader log=%v", peer, raft.log.String())
			return
		}

		// 成功下发日志, 更新呢 提交进度;
		raft.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		raft.nextIndex[peer] = raft.matchIndex[peer] + 1

		majorityIndex := raft.getMajorityIndexLocked()
		if majorityIndex > raft.commitIndex && raft.log.at(majorityIndex).Term == raft.currentTerm {
			Log(raft.me, raft.currentTerm, DApply, "Leader update the commit index %d->%d", raft.commitIndex, majorityIndex)
			raft.commitIndex = majorityIndex
			raft.applyCond.Signal()
		}
	}

	raft.mux.Lock()
	defer raft.mux.Unlock()

	if raft.contextLockIsLost(leader, term) {
		Log(raft.me, raft.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, raft.role, raft.currentTerm)
		return false
	}

	for i := 0; i < len(raft.peers); i++ {
		if i == raft.me {
			raft.nextIndex[i] = raft.log.size()
			raft.matchIndex[i] = raft.log.size() - 1
			continue
		}

		// 找到前一个日志索引
		preIndex := raft.nextIndex[i] - 1

		// 如果前一个日志索引, 被截断, 则重置为日志的末尾;
		if raft.log.snapshotIndex > preIndex {
			args := &InstallSnapshotArgs{
				Term:              raft.currentTerm,
				LeaderId:          raft.me,
				LastIncludedIndex: raft.log.snapshotIndex,
				LastIncludedTerm:  raft.log.snapshotTerm,
				Snapshot:          raft.log.snapshot,
			}
			Log(raft.me, raft.currentTerm, DDebug, "-> S%d, SendSnap, Args=%v", i, args.String())
			go raft.installSnapshotToPeer(i, term, args)
			continue
		}

		prevTerm := raft.log.at(preIndex).Term
		args := &AppendEntriesArgs{
			Term:         raft.currentTerm,
			LeaderId:     raft.me,
			PrevLogIndex: preIndex,
			PrevLogTerm:  prevTerm,
			LeaderCommit: raft.commitIndex,
			Entries:      raft.log.tail(preIndex + 1),
		}
		Log(raft.me, raft.currentTerm, DDebug, "-> S%d, Append, Args=%v", i, args.String())
		go replicateToPeer(i, args)
	}
	//return false
	return true
}

func (raft *Raft) replicationTicker(term int) {
	for !raft.Killed() {
		ok := raft.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}

func (raft *Raft) getMajorityIndexLocked() int {
	tmpIndex := make([]int, len(raft.peers))
	copy(tmpIndex, raft.matchIndex)
	sort.Ints(tmpIndex)
	majorityIdx := (len(raft.peers) - 1) / 2
	Log(raft.me, raft.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndex, majorityIdx, tmpIndex[majorityIdx])
	return tmpIndex[majorityIdx]
}
