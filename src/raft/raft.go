package raft

import (
	"raftCourse/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 400 * time.Millisecond

	replicateInterval = 30 * time.Millisecond
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)

type Role string

const (
	follower  Role = "follower"
	candidate Role = "candidate"
	leader    Role = "leader"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Raft struct {
	mux       sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	role        Role
	currentTerm int
	votedFor    int

	log *RaftLog

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	applyChan   chan ApplyMsg
	applyCond   *sync.Cond
	snapPending bool

	electionStart   time.Time
	electionTimeout time.Duration
}

func (raft *Raft) becomeFollowerLocked(term int) {
	if term < raft.currentTerm {
		Log(raft.me, raft.currentTerm, DError, "Can`t become follower,low term: T%d", term)
		return
	}
	Log(raft.me, raft.currentTerm, DLog, "%s->follower, for T%v -> T%v", raft.role, raft.currentTerm, term)
	raft.role = follower
	shouldPersit := raft.currentTerm != term
	if term > raft.currentTerm {
		raft.votedFor = -1
	}
	raft.currentTerm = term
	if shouldPersit {
		raft.persistLocked()
	}
}

func (raft *Raft) becomeCandidateLocked() {
	if raft.role == leader {
		Log(raft.me, raft.currentTerm, DError, "Leader can't become Candidate")
		return
	}
	Log(raft.me, raft.currentTerm, DVote, "%s->Candidate, For T%d", raft.role, raft.currentTerm+1)
	raft.currentTerm++
	raft.role = candidate
	raft.votedFor = raft.me
	raft.persistLocked()
}

func (raft *Raft) becomeLeaderLocked() {
	if raft.role != candidate {
		Log(raft.me, raft.currentTerm, DError, "Only Candidate can become Leader")
		return
	}
	Log(raft.me, raft.currentTerm, DLeader, "Become Leader in T%d", raft.currentTerm)
	raft.role = leader
	for i := 0; i < len(raft.peers); i++ {
		raft.nextIndex[i] = raft.log.size()
		raft.matchIndex[i] = 0
	}
}

func (raft *Raft) GetState() (int, bool) {
	raft.mux.Lock()
	defer raft.mux.Unlock()
	return raft.currentTerm, raft.role == leader
}

func (raft *Raft) GetRaftStateSize() int {
	raft.mux.Lock()
	defer raft.mux.Unlock()
	return raft.persister.RaftStateSize()
}

func (raft *Raft) Start(command interface{}) (int, int, bool) {
	raft.mux.Lock()
	defer raft.mux.Unlock()

	if raft.role != leader {
		return 0, 0, false
	}

	raft.log.append(LogEntry{
		Term:         raft.currentTerm,
		CommandValid: true,
		Command:      command,
	})

	Log(raft.me, raft.currentTerm, DLeader, "Leader accept log[%d]T%d", raft.log.size()-1, raft.currentTerm)

	raft.persistLocked()
	return raft.log.size() - 1, raft.currentTerm, true
}

func (raft *Raft) Kill() {
	atomic.StoreInt32(&raft.dead, 1)
}

func (raft *Raft) Killed() bool {
	return atomic.LoadInt32(&raft.dead) == 1
}

func (raft *Raft) contextLockIsLost(role Role, term int) bool {
	return !(raft.role == role && raft.currentTerm == term)
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyChan chan ApplyMsg) *Raft {
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.me = me

	raft.role = follower
	raft.currentTerm = 1
	raft.votedFor = -1

	raft.log = NewRaftLog(InvalidIndex, InvalidTerm, nil, nil)

	raft.nextIndex = make([]int, len(peers))
	raft.matchIndex = make([]int, len(peers))

	raft.applyChan = applyChan
	raft.applyCond = sync.NewCond(&raft.mux)

	raft.commitIndex = 0
	raft.lastApplied = 0
	raft.snapPending = false

	raft.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go raft.electionTicker()

	// 开启周期提交日志的goroutine
	go raft.applicationTicker()

	return raft
}
