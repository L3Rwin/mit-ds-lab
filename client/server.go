package kvraft

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"MIT6.824-6.5840/labrpc"
	"MIT6.824-6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // Index forwarded from the Raft service layer
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	maxId     map[int64]int               // Ensure each seq runs only once: clientId / seqId
	waitChMap map[int]chan Op             // Forward commands from Raft applyCh: index / chan(Op)
	kvPersist map[string]*strings.Builder // Persisted KV pairs: K / V
	getCache  map[int64]map[string]string // Cache for Get: clientId / Key / Value
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	register()
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	// You may need initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0

	kv.maxId = make(map[int64]int)

	kv.waitChMap = make(map[int]chan Op)
	kv.kvPersist = make(map[string]*strings.Builder)
	kv.getCache = make(map[int64]map[string]string)

	DPrintf(1111, "[Server][%v] KVServer created", kv.me)

	go func() {
		for msg := range kv.applyCh {
			if kv.killed() {
				return
			}
			if !msg.CommandValid {
				continue
			}
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			var ch chan Op
			kv.mu.Lock()
			if op.SeqId > kv.maxId[op.ClientId] {
				switch op.OpType {
				case PutOp:
					b := &strings.Builder{}
					b.WriteString(op.Value)
					kv.kvPersist[op.Key] = b
				case AppendOp:
					if b, ok := kv.kvPersist[op.Key]; ok {
						b.WriteString(op.Value)
					} else {
						b := &strings.Builder{}
						b.WriteString(op.Value)
						kv.kvPersist[op.Key] = b
					}
				case GetOp:
					// Read after apply to populate cache for duplicate Gets.
					if _, ok := kv.getCache[op.ClientId]; !ok {
						kv.getCache[op.ClientId] = make(map[string]string)
					}
					if b, ok := kv.kvPersist[op.Key]; ok {
						op.Value = b.String()
					} else {
						op.Value = ""
					}
					kv.getCache[op.ClientId][op.Key] = op.Value
				}
				kv.maxId[op.ClientId] = op.SeqId
			} else if op.OpType == GetOp {
				// Fill Value for duplicate Gets so RPC can reply.
				if cache, ok := kv.getCache[op.ClientId]; ok {
					op.Value = cache[op.Key]
				} else if b, ok := kv.kvPersist[op.Key]; ok {
					op.Value = b.String()
				} else {
					op.Value = ""
				}
			}
			if idx, ok := kv.waitChMap[msg.CommandIndex]; ok {
				ch = idx
				delete(kv.waitChMap, msg.CommandIndex)
				DPrintf(1111, "[Server][%v] Command index: %v forwarded", kv.me, msg.CommandIndex)
			}
			kv.mu.Unlock()

			if ch != nil {
				ch <- op
				close(ch)
			}
		}
	}()
	return kv

}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	// Your code here.
	if args.SeqId <= kv.maxId[args.ClientId] {
		reply.Err = ErrDuplicate
		if cache, ok := kv.getCache[args.ClientId]; ok {
			reply.Value = cache[args.Key]
		} else {
			reply.Value = ""
		}
		DPrintf(1111, "[Server][%v] Get from client: %v index: %v duplicate, supposed to be %v", kv.me, args.ClientId, args.SeqId, kv.maxId[args.ClientId]+1)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{
		SeqId:    args.SeqId,
		Key:      args.Key,
		ClientId: args.ClientId,
		OpType:   GetOp,
	}
	idx, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan Op, 1)
	kv.mu.Lock()
	DPrintf(1111, "[Server][%v] Get from client: %v index: %v started, key: %v", kv.me, args.ClientId, idx, args.Key)
	kv.waitChMap[idx] = ch

	kv.mu.Unlock()
	start := time.Now()

	for time.Since(start) < ApplyTimeout {
		_, ld := kv.rf.GetState()
		if !ld {
			DPrintf(1111, "[Server][%v] Get from client: %v index: %v leader lost", kv.me, args.ClientId, idx)
			kv.mu.Lock()
			delete(kv.waitChMap, idx)
			kv.mu.Unlock()
			reply.Err = ErrWrongLeader
			return
		}
		select {

		case applied := <-ch:
			DPrintf(1111, "[Server][%v] Get from client: %v index: %v reply received", kv.me, args.ClientId, idx)
			if applied.ClientId != args.ClientId || applied.SeqId != args.SeqId || applied.OpType != GetOp {
				reply.Err = ErrWrongLeader
				return
			}
			reply.Err = OK
			reply.Value = applied.Value
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}

	}
	kv.mu.Lock()
	delete(kv.waitChMap, idx)
	kv.mu.Unlock()
	reply.Err = ErrTimeout

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	// Your code here.
	if args.SeqId <= kv.maxId[args.ClientId] {
		reply.Err = ErrDuplicate
		DPrintf(1111, "[Server][%v] PutAppend from client: %v index: %v duplicate, supposed to be %v", kv.me, args.ClientId, args.SeqId, kv.maxId[args.ClientId]+1)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op := Op{
		SeqId:    args.SeqId,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpType:   args.Op,
	}
	idx, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		// DPrintf(1111, "[Server][%v] PutAppend from client: %v index: %v wrong leader", kv.me, args.ClientId, idx)
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan Op, 1)

	kv.mu.Lock()
	DPrintf(1111, "[Server][%v] PutAppend from client: %v index: %v started, key: %v, op: %v", kv.me, args.ClientId, idx, args.Key, args.Op)
	kv.waitChMap[idx] = ch
	kv.mu.Unlock()
	start := time.Now()

	for time.Since(start) < ApplyTimeout {
		_, ld := kv.rf.GetState()
		if !ld {
			DPrintf(1111, "[Server][%v] PutAppend from client: %v index: %v leader lost", kv.me, args.ClientId, idx)
			kv.mu.Lock()
			delete(kv.waitChMap, idx)
			kv.mu.Unlock()
			reply.Err = ErrWrongLeader
			return
		}
		select {
		case applied := <-ch:
			DPrintf(1111, "[Server][%v] PutAppend from client: %v index: %v reply received", kv.me, args.ClientId, idx)
			if applied.ClientId != args.ClientId || applied.SeqId != args.SeqId || applied.OpType != args.Op {
				reply.Err = ErrWrongLeader
				return
			}
			reply.Err = OK
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}

	}
	kv.mu.Lock()
	delete(kv.waitChMap, idx)
	kv.mu.Unlock()
	reply.Err = ErrTimeout

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	DPrintf(11, "%v: is killed", kv)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
