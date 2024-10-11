package kvraft

import "time"

const (
	OK             = "OK"
	ErrNotKey      = "ErrNotKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type OpType string

const (
	onPut    OpType = "put"
	onAppend OpType = "append"
)

type Err string

type PutAppendArg struct {
	Key      string
	Value    string
	Op       OpType //  "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArg struct {
	Key string
}

type GetReply struct {
	Value string
	Err   Err
}

const ClientRequestTimeout = 500 * time.Millisecond

const Debug = false

type CommandOpType uint8

const (
	OpGet CommandOpType = iota
	OpPut
	OpAppend
)

type Command struct {
	Key      string
	Value    string
	ComType  CommandOpType
	ClientId int64
	SeqId    int64
}

type CommandReply struct {
	Value string
	Err   Err
}

func getCommandOpType(v OpType) CommandOpType {
	switch v {
	case onPut:
		return OpPut
	case onAppend:
		return OpAppend
	default:
		panic("unknown command type")
	}
}

type LastOperationInfo struct {
	SeqId int64
	Reply *CommandReply
}
