package raft

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()
		applyMsgs := make([]ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsgs = append(applyMsgs, ApplyMsg{
				Command:      rf.log[i].Command,
				CommandValid: rf.log[i].CommandValid,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()

		for _, msg := range applyMsgs {
			rf.applyCh <- msg
		}
		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(applyMsgs))
		rf.lastApplied = rf.lastApplied + len(applyMsgs)
		rf.mu.Unlock()
	}
}
