package kvraft

import (
	"bytes"
	"fmt"
	"raftCourse/labgob"
	"raftCourse/labrpc"
	"raftCourse/raft"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mux     sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxRaftStateSize int

	lastAppliedIndex int
	stateMachine     *MemoryKVStateMachine
	notifyChanMap    map[int]chan *CommandReply
	duplicateMap     map[int64]LastOperationInfo
}

func StartKVServer(server []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftStateSize = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(server, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastAppliedIndex = 0
	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyChanMap = make(map[int]chan *CommandReply)
	kv.duplicateMap = make(map[int64]LastOperationInfo)

	kv.restoreFromSnapshot(persister.ReadSnapshot())

	go kv.applyTask()
	return kv
}

func (s *KVServer) Get(args *GetArg, reply *GetReply) {
	index, _, isLeader := s.rf.Start(Command{
		Key:     args.Key,
		ComType: OpGet})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	s.mux.Lock()
	notifyChannel := s.getNotifyChannel(index)
	s.mux.Unlock()

	select {
	case result := <-notifyChannel:
		fmt.Sprintf("reply:%v \n", reply)
		fmt.Sprintf("result:%v \n", result)
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeOut
	}

	go func() {
		s.mux.Lock()
		s.removeNotifyChannel(index)
		s.mux.Unlock()
	}()
}

func (s *KVServer) requestDuplicated(clientId int64, seqId int64) bool {
	if info, ok := s.duplicateMap[clientId]; ok {
		return seqId <= info.SeqId
	}
	return false
}

func (s *KVServer) PutAppend(args *PutAppendArg, reply *PutAppendReply) {
	s.mux.Lock()
	if s.requestDuplicated(args.ClientId, args.SeqId) {
		comReply := s.duplicateMap[args.ClientId].Reply
		reply.Err = comReply.Err
		s.mux.Unlock()
		return
	}
	s.mux.Unlock()

	index, _, isLeader := s.rf.Start(Command{
		Key:      args.Key,
		Value:    args.Value,
		ComType:  getCommandOpType(args.Op),
		ClientId: args.ClientId,
		SeqId:    args.SeqId})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	s.mux.Lock()
	notifyChannel := s.getNotifyChannel(index)
	s.mux.Unlock()

	select {
	case result := <-notifyChannel:
		fmt.Sprintf("reply:%v \n", reply)
		fmt.Sprintf("result:%v \n", result)
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeOut
	}
	// 删除通知的 channel
	go func() {
		s.mux.Lock()
		s.removeNotifyChannel(index)
		s.mux.Unlock()
	}()
}

// 应用层收集最终有效消息, apply 任务
func (s *KVServer) applyTask() {
	for !s.killed() {
		select {
		// raft 模块向此通道发送数据;
		case message := <-s.applyCh:
			if message.CommandValid {
				s.mux.Lock()
				// 如果是已经处理过的消息则直接忽略
				if s.lastAppliedIndex >= message.CommandIndex {
					s.mux.Unlock()
					continue
				}

				s.lastAppliedIndex = message.CommandIndex

				// 取出用户的操作信息
				command := message.Command.(Command)
				var comReply *CommandReply
				// 先 put append 查重
				if command.ComType != OpGet && s.requestDuplicated(command.ClientId, command.SeqId) {
					comReply = s.duplicateMap[command.ClientId].Reply
				} else { // get put append
					// 将操作应用状态机中
					//opReply := s.applyToStateMachine(command)
					comReply = s.applyToStateMachine(command)
					if command.ComType != OpGet {
						//  put append 操作进行保存
						s.duplicateMap[command.ClientId] = LastOperationInfo{
							SeqId: command.SeqId,
							Reply: comReply,
						}
					}
				}

				// 将结果发送回去
				if _, isLeader := s.rf.GetState(); isLeader {
					notifyChannel := s.getNotifyChannel(message.CommandIndex)
					notifyChannel <- comReply
				}

				// 判断是否需要 snapshot
				if s.maxRaftStateSize != -1 && s.rf.GetRaftStateSize() >= s.maxRaftStateSize {
					s.makeSnapshot(message.CommandIndex)
				}
				s.mux.Unlock()

			} else if message.SnapshotValid {
				s.mux.Lock()
				s.restoreFromSnapshot(message.Snapshot)
				s.lastAppliedIndex = message.SnapshotIndex
				s.mux.Unlock()
			}
		}
	}
}

func (s *KVServer) applyToStateMachine(command Command) *CommandReply {
	var value string
	var err Err
	switch command.ComType {
	case OpGet:
		value, err = s.stateMachine.Get(command.Key)
	case OpPut:
		err = s.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = s.stateMachine.Append(command.Key, command.Value)
	}
	return &CommandReply{
		Value: value,
		Err:   err,
	}
}

func (s *KVServer) getNotifyChannel(index int) chan *CommandReply {
	if _, ok := s.notifyChanMap[index]; !ok {
		s.notifyChanMap[index] = make(chan *CommandReply, 1)
	}
	return s.notifyChanMap[index]
}

func (s *KVServer) removeNotifyChannel(index int) {
	delete(s.notifyChanMap, index)
}

func (s *KVServer) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(s.stateMachine)
	e.Encode(s.duplicateMap)
	s.rf.Snapshot(index, buf.Bytes())
}

func (s *KVServer) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(buf)
	// var stateMachine *MemoryKVStateMachine
	var stateMachine MemoryKVStateMachine
	var duplicateMap map[int64]LastOperationInfo
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&duplicateMap) != nil {
		panic("failed to restore state from snapshot")
	}
	s.stateMachine = &stateMachine
	s.duplicateMap = duplicateMap
}

func (s *KVServer) Kill() {
	atomic.StoreInt32(&s.dead, 1)
	s.rf.Kill()
}

func (s *KVServer) killed() bool {
	return atomic.LoadInt32(&s.dead) == 1
}
