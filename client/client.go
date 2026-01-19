package kvraft

import (
	"sync/atomic"
	"time"

	"MIT6.824-6.5840/labrpc"
)

var clientId int64 = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	seqId    int
	leaderId int // Track leader; send next request directly to it
	clientId int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	DPrintf(1111, "[Client][%v] Clerk created", clientId)
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leaderId = int(nrand()) % len(ck.servers)
	ck.seqId = 0
	ck.clientId = clientId
	atomic.AddInt64(&clientId, 1)
	return ck
}
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	DPrintf(1111, "[Client][%v] Get index: %v called, key: %v", ck.clientId, ck.seqId, key)
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {

		reply := GetReply{}
		//fmt.Printf("[ ++++Client[%v]++++] : send a Get,args:%+v,serverId[%v]\n", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err == ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = serverId
		DPrintf(1111, "[Client][%v] Get index: %v replied, key: %v, reply: %+v", ck.clientId, ck.seqId, key, reply.Err)
		if reply.Err == ErrTimeout || reply.Err == ErrOutOfOrder {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if reply.Err == OK || reply.Err == ErrDuplicate {

			return reply.Value
		}
		if reply.Err == ErrNoKey {
			return ""
		}

	}

}
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	DPrintf(1111, "[Client][%v] PutAppend index: %v called, key: %v, op: %v", ck.clientId, ck.seqId, key, op)
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, SeqId: ck.seqId, Op: op}
	serverId := ck.leaderId
	for {

		reply := PutAppendReply{}
		//fmt.Printf("[ ++++Client[%v]++++] : send a Get,args:%+v,serverId[%v]\n", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		ck.leaderId = serverId
		DPrintf(1111, "[Client][%v] PutAppend index: %v replied, key: %v, op: %v, reply: %+v", ck.clientId, ck.seqId, key, op, reply.Err)

		if reply.Err == ErrTimeout || reply.Err == ErrOutOfOrder {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if reply.Err == OK || reply.Err == ErrDuplicate {

			return
		}

	}

}

func (ck *Clerk) Put(key string, value string) {

	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {

	ck.PutAppend(key, value, AppendOp)
}
