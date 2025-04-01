领导者选举
    是否已经成为了leader || 是否已经到了选举超时
        将自己的最大日志索引发送给其他节点;
     对端节点进行判断,结合日志来判断是否回应;持久化信息;重置选举器;
    是否超过半数的节点是否都同意,同意的话就成为leader,并开始日志复制任务;

持久化元信息
    term votedFor snapshot
    raftLog.persist(e)
    save(raftState, snapshot) 
        

日志复制
  依次选择各个节点, 假如是自己节点,那么就更新nextIndex[]matchIndex[]为最大值;
  到节点1,假如leader将要发送的日志索引prevIdx 被快照, 就先发送快照; 根据结果更新nextIndex[]matchIndex[];
    节点1接收快照后,设置相关SnapshotIndex,term; log[]重新初始化,并持久化信息; 
  快照完毕,继续下一个节点;
  发送普通日志,全局Index:
      接收日志节点返回:
        L1.日志失败:如果 prevIndex 大于 本地的最大日志,说明本地日志太短,那么就返回本地的最大日志,并返回0任期;
        L2.日志失败:如果 prevIndex 小于 本地的快照日志,返回快照的最后一个日志索引和快照的最后一个任期值;
        L3.任期失败:prevIndex有entry,就比较任期, 如果不等于任期, 返回本地此任期,和此任期的第一个日志;
        L4.成功:从上一个日志处追加参数日志,持久化,根据leader的commitIndex和来更新本地commit;   
Leader节点根据返回的结果: 来进行回退检测;
L4 成功: 更新nextIndex[]matchIndex[]; 并且根据majorityMatched 和其任期来更新 commit; 
   失败: L1: 0任期 size()索引 -> 把 next index 设置为 follower 发回的 last log index + 1
        L2: snapLastTerm任期snapLastIdx索引 -> 把 next index 设置为 follower 发回的 last log index + 1
        L3: 如果 reply 为 term not matched，leader 会在自己的 log 中找是否有 conflict term 的 log entry
            - 如果找到了，则会把 next index 设置为 leader log 中 term 为 conflict term 的最后一个 log entry 的 index
            - 如果没有找到，则会把 next index 设置为 follower 发回的 first conflict index

日志应用



