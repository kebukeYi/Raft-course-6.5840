package kvraft

import (
	"crypto/rand"
	"math/big"
	"raftCourse/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int // 记录 Leader 节点的 id，避免下一次请求的时候去轮询查找 Leader
	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	nbigx, _ := rand.Int(rand.Reader, max)
	i := nbigx.Int64()
	return i
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (c *Clerk) Get(key string) string {
	args := GetArg{
		Key: key,
	}
	for {
		var getReply GetReply
		ok := c.servers[c.leaderId].Call("KVServer.Get", &args, &getReply)
		if !ok || getReply.Err == ErrWrongLeader || getReply.Err == ErrTimeOut {
			c.leaderId = (c.leaderId + 1) % len(c.servers)
			continue
		}
		return getReply.Value
	}
}

func (c *Clerk) Put(key, value string) {
	c.PutAppend(key, value, onPut)
}

func (c *Clerk) Append(key, value string) {
	c.PutAppend(key, value, onAppend)
}
func (c *Clerk) PutAppend(key, value string, opType OpType) {
	args := PutAppendArg{
		Key:      key,
		Value:    value,
		Op:       opType,
		ClientId: c.clientId,
		SeqId:    c.seqId,
	}
	for {
		var reply PutAppendReply
		ok := c.servers[c.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			c.leaderId = (c.leaderId + 1) % len(c.servers)
			continue
		}
		c.seqId++
		return
	}
}
