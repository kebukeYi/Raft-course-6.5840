package raft

import (
	"bytes"
	"fmt"
	"raftCourse/labgob"
)

func (raft *Raft) persistString() string {
	return fmt.Sprintf("currentTerm=%d, votedFor=%d, log: [0: %d]",
		raft.currentTerm, raft.votedFor, raft.log.size())
}

func (raft *Raft) persistLocked() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(raft.currentTerm)
	e.Encode(raft.votedFor)
	// 日志: 快照元信息+操作指令
	raft.log.persist(e)
	raftState := w.Bytes()
	// 快照的生成是应用层生成; 只需要保存即可;
	raft.persister.Save(raftState, raft.log.snapshot)
	Log(raft.me, raft.currentTerm, DPersist,
		"Persist: %v", raft.persistString())
}

func (raft *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	// r := new(bytes.Buffer) 错误写法
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		Log(raft.me, raft.currentTerm, DPersist,
			"Read currentTerm error: %v", err)
		return
	}
	raft.currentTerm = currentTerm

	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		Log(raft.me, raft.currentTerm, DPersist, "Read votedFor error: %v", err)
		return
	}
	raft.votedFor = votedFor

	if err := raft.log.readPersist(d); err != nil {
		Log(raft.me, raft.currentTerm, DPersist, "Read log error: %v", err)
		return
	}

	// 独立读取快照
	raft.log.snapshot = raft.persister.ReadSnapshot()

	// 从磁盘中读取的快照日志 > 提交索引
	if raft.log.snapshotIndex > raft.commitIndex {
		raft.commitIndex = raft.log.snapshotIndex
		raft.lastApplied = raft.log.snapshotIndex
	}
	Log(raft.me, raft.currentTerm, DPersist, "Read from persist: %v", raft.persistString())
}
