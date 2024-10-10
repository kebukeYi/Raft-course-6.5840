package raft

func (raft *Raft) applicationTicker() {
	for !raft.Killed() {
		raft.mux.Lock()
		raft.applyCond.Wait()

		entries := make([]LogEntry, 0)
		snapPendingApply := raft.snapPending

		// 先设置好 要应用的日志索引
		if !snapPendingApply {
			if raft.log.snapshotIndex > raft.lastApplied {
				raft.lastApplied = raft.log.snapshotIndex
			}

			start := raft.lastApplied + 1
			end := raft.commitIndex
			if end >= raft.log.size() {
				end = raft.log.size() - 1
			}
			for i := start; i <= end; i++ {
				entries = append(entries, raft.log.at(i))
			}
		}
		raft.mux.Unlock()

		// 发送数据,无锁;
		if !snapPendingApply {
			for i, entry := range entries {
				raft.applyChan <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: raft.lastApplied + i + 1,
				}
			}
		} else {
			raft.applyChan <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      raft.log.snapshot,
				SnapshotIndex: raft.log.snapshotIndex,
				SnapshotTerm:  raft.log.snapshotTerm,
			}
		}

		// 加锁, 更新apply进度
		raft.mux.Lock()
		if !snapPendingApply {
			Log(raft.me, raft.currentTerm, DApply, "Apply log for [%d, %d]", raft.lastApplied+1, raft.lastApplied+len(entries))
			raft.lastApplied += len(entries)
		} else {
			Log(raft.me, raft.currentTerm, DApply, "Apply snapshot for [0, %d]", 0, raft.log.snapshotIndex)
			raft.lastApplied = raft.log.snapshotIndex
			if raft.lastApplied > raft.commitIndex {
				raft.commitIndex = raft.lastApplied
			}
			// 快照已经成功应用
			raft.snapPending = false
		}
		raft.mux.Unlock()
	}
}
