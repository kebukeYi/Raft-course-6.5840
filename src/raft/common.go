package raft

import "time"

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	replicateInterval  time.Duration = 70 * time.Millisecond
)

type Role string

const (
	Follower  Role = "Follower"
	Leader    Role = "Leader"
	Candidate Role = "Candidate"
)

const (
	InvalidTerm  int = 0
	InvalidIndex int = 0
)
