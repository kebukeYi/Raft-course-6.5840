package raft

import (
	"fmt"
	"raftCourse/labgob"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int
	snapshot      []byte
	tailLog       []LogEntry
}

func NewRaftLog(snapIndex int, snapTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	raftLog := &RaftLog{
		snapLastIndex: snapIndex,
		snapLastTerm:  snapTerm,
		snapshot:      snapshot,
	}

	raftLog.tailLog = make([]LogEntry, 0, 1+len(entries))
	raftLog.tailLog = append(raftLog.tailLog, LogEntry{Term: snapTerm})
	raftLog.tailLog = append(raftLog.tailLog, entries...)
	return raftLog
}

func (rfl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rfl.snapLastIndex)
	e.Encode(rfl.snapLastTerm)
	e.Encode(rfl.tailLog)
}

func (rfl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rfl.snapLastIndex = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term failed")
	}
	rfl.snapLastTerm = lastTerm

	var tailLog []LogEntry
	if err := d.Decode(&tailLog); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rfl.tailLog = tailLog
	return nil
}

func (rfl *RaftLog) doSnapShot(index int, snapShot []byte) {
	idx := rfl.idx(index)
	rfl.snapLastTerm = rfl.tailLog[idx].Term
	rfl.snapLastIndex = index
	rfl.snapshot = snapShot
	newLog := make([]LogEntry, 0, rfl.size()-rfl.snapLastIndex)
	newLog = append(newLog, LogEntry{Term: rfl.snapLastTerm})
	newLog = append(newLog, rfl.tailLog[idx+1:]...)
	rfl.tailLog = newLog
}

func (rfl *RaftLog) installSnapShot(index, term int, snapShot []byte) {
	rfl.snapLastIndex = index
	rfl.snapLastTerm = term
	rfl.snapshot = snapShot
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{Term: rfl.snapLastTerm})
	rfl.tailLog = newLog
}

func (rfl *RaftLog) size() int {
	return rfl.snapLastIndex + len(rfl.tailLog)
}

func (rfl *RaftLog) idx(logicIndex int) int {
	if logicIndex >= rfl.size() || rfl.snapLastIndex > logicIndex {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIndex, rfl.snapLastIndex, rfl.size()-1))
	}
	return logicIndex - rfl.snapLastIndex
}

func (rfl *RaftLog) at(logicIndex int) LogEntry {
	return rfl.tailLog[rfl.idx(logicIndex)]
}

// more detailed
func (rfl *RaftLog) String() string {
	var terms string
	prevTerm := rfl.snapLastTerm
	prevStart := rfl.snapLastIndex
	for i := 0; i < len(rfl.tailLog); i++ {
		if rfl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, i-1, prevTerm)
			prevTerm = rfl.tailLog[i].Term
			prevStart = rfl.snapLastIndex + i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, rfl.snapLastIndex+len(rfl.tailLog)-1, prevTerm)
	return terms
}

func (rfl *RaftLog) Str() string {
	lastIdx, lastTerm := rfl.last()
	return fmt.Sprintf("[%d]T%d~[%d]T%d", rfl.snapLastIndex, rfl.snapLastTerm, lastIdx, lastTerm)
}

func (rfl *RaftLog) last() (int, int) {
	return rfl.size() - 1, rfl.tailLog[len(rfl.tailLog)-1].Term
}

func (rfl *RaftLog) tail(startIndex int) []LogEntry {
	if startIndex >= rfl.size() {
		return nil
	}
	return rfl.tailLog[rfl.idx(startIndex):]
}

func (rfl *RaftLog) firstForTerm(term int) int {
	for idx, entry := range rfl.tailLog {
		if entry.Term == term {
			return idx + rfl.snapLastIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rfl *RaftLog) append(e LogEntry) {
	rfl.tailLog = append(rfl.tailLog, e)
}

func (rfl *RaftLog) truncate(endIndex int) {
	rfl.tailLog = rfl.tailLog[:rfl.idx(endIndex)]
}

func (rfl *RaftLog) appendFrom(startIndex int, entries []LogEntry) {
	rfl.tailLog = append(rfl.tailLog[:rfl.idx(startIndex)+1], entries...)
}
