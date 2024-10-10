package raft

import "fmt"

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

func (raft *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	raft.mux.Lock()
	defer raft.mux.Unlock()
	Log(raft.me, raft.currentTerm, DDebug, "<- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = raft.currentTerm

	// align the term
	if raft.currentTerm > args.Term {
		Log(raft.me, raft.currentTerm, DSnap, "<- S%d, Reject Snap, Higher Term: T%d>T%d", args.LeaderId, raft.currentTerm, args.Term)
		return
	}

	if args.Term >= raft.currentTerm {
		raft.becomeFollowerLocked(args.Term)
	}

	if raft.log.snapshotIndex >= args.LastIncludedIndex {
		Log(raft.me, raft.currentTerm, DSnap, "<- S%d, Reject Snap, Already installed: %d>%d", args.LeaderId, raft.log.snapshotIndex, args.LastIncludedIndex)
		return
	}

	// install the snapshot in the memory/persist/app
	raft.log.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	raft.persistLocked()
	raft.snapPending = true
	raft.applyCond.Signal()
}

func (raft *Raft) installSnapshotToPeer(peer, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := raft.sendInstallSnapshot(peer, args, reply)
	raft.mux.Lock()
	defer raft.mux.Unlock()

	if !ok {
		Log(raft.me, raft.currentTerm, DDebug, "-> S%d, InstallSnapshot, Lost or error", peer)
		return
	}
	Log(raft.me, raft.currentTerm, DDebug, "-> S%d, InstallSnapshot, Reply=%v", peer, reply.String())

	if reply.Term > raft.currentTerm {
		raft.becomeFollowerLocked(reply.Term)
		return
	}

	if raft.contextLockIsLost(leader, term) {
		Log(raft.me, raft.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, raft.currentTerm, raft.role)
		return
	}

	// 下发快照后需更新nextIndex
	if args.LastIncludedIndex > raft.matchIndex[peer] {
		raft.matchIndex[peer] = args.LastIncludedIndex
		raft.nextIndex[peer] = raft.matchIndex[peer] + 1
	}
}

func (raft *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := raft.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (raft *Raft) Snapshot(index int, snapshot []byte) {
	raft.mux.Lock()
	defer raft.mux.Unlock()
	// leader的提交进度可能大于某些节点的commitIndex, 因此 当有些节点安装快照时,会一并更新 commitIndex;
	// 只允许 快照在commitIndex之前调用
	if index > raft.commitIndex {
		Log(raft.me, raft.currentTerm, DSnap, "Couldn't snapshot before CommitIdx: %d>%d", index, raft.commitIndex)
		return
	}
	// 避免重复安装快照
	if index <= raft.log.snapshotIndex {
		Log(raft.me, raft.currentTerm, DSnap, "Already snapshot in %d<=%d", index, raft.log.snapshotIndex)
		return
	}
	// 保存快照
	raft.log.doSnapshot(index, snapshot)
	raft.persistLocked()
}
