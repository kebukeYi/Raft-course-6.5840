package raft

import (
	"fmt"
	"raftCourse/labgob"
)

type RaftLog struct {
	snapshotIndex int
	snapshotTerm  int

	snapshot []byte
	tailLog  []LogEntry
}

func NewRaftLog(snapLastIndex, snapLastTerm int, snapshot []byte,
	enrties []LogEntry) *RaftLog {

	rl := &RaftLog{
		snapshotIndex: snapLastIndex,
		snapshotTerm:  snapLastTerm,
		snapshot:      snapshot,
	}

	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, enrties...)

	return rl
}
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIndex int
	if err := d.Decode(&lastIndex); err != nil {
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapshotIndex = lastIndex

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.snapshotTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log
	return nil
}

// raftLog 日志模块的持久化
func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	// index 逐步递增
	e.Encode(rl.snapshotIndex)
	e.Encode(rl.snapshotTerm)
	e.Encode(rl.tailLog)
}
func (rl *RaftLog) size() int {
	return rl.snapshotIndex + len(rl.tailLog)
}

// logicIndex:全量索引index
func (rl *RaftLog) idx(logicIndex int) int {
	// 太小 || 太大
	//if logicIndex < rl.snapshotIndex || logicIndex > rl.size() {
	if logicIndex < rl.snapshotIndex || logicIndex >= rl.size() {
		panic(fmt.Sprintf("%d is out of [%d, %d]", logicIndex, rl.snapshotIndex, rl.size()-1))
	}
	return logicIndex - rl.snapshotIndex
}
func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}
func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapshotIndex + i, rl.tailLog[i].Term
}
func (rl *RaftLog) firstFor(term int) int {
	// tailLog的第一个元素是 快照的最后一个元素,额外保存了;
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapshotIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}
func (rl *RaftLog) tail(startIndex int) []LogEntry {
	if startIndex >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIndex):]
}
func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}
func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPrevIndex)+1], entries...)
}
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapshotTerm
	prevIndex := rl.snapshotIndex
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevIndex, rl.snapshotIndex+i-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevIndex = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevIndex, rl.snapshotIndex+len(rl.tailLog)-1, prevTerm)
	return terms
}

// 应用层调用
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	// 是否小于上次的 快照进度
	if rl.snapshotIndex >= index {
		return
	}

	idx := rl.idx(index)

	rl.snapshotIndex = index
	rl.snapshotTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, rl.size()-rl.snapshotIndex)
	newLog = append(newLog, LogEntry{Term: rl.snapshotTerm})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}
func (rl *RaftLog) installSnapshot(index, term int, snapshot []byte) {
	rl.snapshotIndex = index
	rl.snapshotTerm = term

	// 分开进行存储
	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{Term: rl.snapshotTerm})

	rl.tailLog = newLog
}
