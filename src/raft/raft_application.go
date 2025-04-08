package raft

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()
		applyMsgs := make([]ApplyMsg, 0)
		snapPending := rf.snapPending
		if !snapPending {
			if rf.rlog.snapLastIndex > rf.lastApplied {
				rf.lastApplied = rf.rlog.snapLastIndex
			}
			// make sure that the rf.log have all the entries
			//start := rf.lastApplied + 1
			//end := rf.commitIndex
			//if end >= rf.rlog.size() {
			//	end = rf.rlog.size() - 1
			//}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsgs = append(applyMsgs, ApplyMsg{
					Command:      rf.rlog.at(i).Command,
					CommandValid: rf.rlog.at(i).CommandValid,
					CommandIndex: i})
			}
		}
		rf.mu.Unlock()

		if !snapPending {
			for _, msg := range applyMsgs {
				rf.applyCh <- msg
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.rlog.snapshot,
				SnapshotTerm:  rf.rlog.snapLastTerm,
				SnapshotIndex: rf.rlog.snapLastIndex,
			}
		}

		rf.mu.Lock()
		if !snapPending {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(applyMsgs))
			rf.lastApplied += len(applyMsgs)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Install Snapshot for [0, %d]", rf.rlog.snapLastIndex)
			rf.lastApplied = rf.rlog.snapLastIndex
			if rf.lastApplied > rf.commitIndex {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		rf.mu.Unlock()
	}
}
