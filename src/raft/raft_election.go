package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (raft *Raft) resetElectionTimerLocked() {
	raft.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	raft.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (raft *Raft) isElectionTimeoutLocked() bool {
	return time.Since(raft.electionStart) > raft.electionTimeout
}

func (raft *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	index, term := raft.log.last()
	Log(raft.me, raft.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", index, term, candidateIndex, candidateTerm)

	if term != candidateTerm {
		return term > candidateTerm
	}
	return index > candidateIndex
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last: [%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
}

func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	raft.mux.Lock()
	defer raft.mux.Unlock()
	Log(raft.me, raft.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	reply.Term = raft.currentTerm
	reply.VoteGranted = false

	if args.Term < raft.currentTerm {
		Log(raft.me, raft.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, raft.currentTerm, args.Term)
		return
	}

	// 拉票时, 任期相等怎么办?
	if args.Term > raft.currentTerm {
		raft.becomeFollowerLocked(args.Term)
	}

	// 假如我投过票了 && 投票的不是你
	if raft.votedFor != -1 && raft.votedFor != args.CandidateId {
		Log(raft.me, raft.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateId, raft.votedFor)
		return
	}

	// 没有投过票
	if raft.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		Log(raft.me, raft.currentTerm, DVote, "<- S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true
	raft.votedFor = args.CandidateId
	raft.persistLocked()
	raft.resetElectionTimerLocked()
	Log(raft.me, raft.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)
}

func (raft *Raft) startElection(term int) {
	receivedVotes := 0
	askForVoteToPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := raft.sendRequestVote(peer, args, reply)

		raft.mux.Lock()
		defer raft.mux.Unlock()
		if !ok {
			Log(raft.me, raft.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
			return
		}
		Log(raft.me, raft.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		if reply.Term > raft.currentTerm {
			raft.becomeFollowerLocked(reply.Term)
			return
		}
		// 查看当前任期和角色是否发生了变更
		if raft.contextLockIsLost(candidate, term) {
			Log(raft.me, raft.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
			return
		}
		if reply.VoteGranted {
			receivedVotes++
			if receivedVotes > len(raft.peers)/2 {
				raft.becomeLeaderLocked()
				go raft.replicationTicker(term)
			}
		}
	}

	raft.mux.Lock()
	defer raft.mux.Unlock()

	if raft.contextLockIsLost(candidate, term) {
		Log(raft.me, raft.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d], abort RequestVote", raft.role, term, raft.currentTerm)
		return
	}

	lastLogIndex, lastLogTerm := raft.log.last()
	for i := 0; i < len(raft.peers); i++ {
		if i == raft.me {
			receivedVotes++
			continue
		}
		args := &RequestVoteArgs{
			Term:         raft.currentTerm,
			CandidateId:  raft.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		Log(raft.me, raft.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", i, args.String())

		go askForVoteToPeer(i, args)
	}
}
func (raft *Raft) electionTicker() {
	for !raft.Killed() {
		raft.mux.Lock()
		if raft.role != leader && raft.isElectionTimeoutLocked() {
			raft.becomeCandidateLocked()
			go raft.startElection(raft.currentTerm)
		}
		raft.mux.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := raft.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
