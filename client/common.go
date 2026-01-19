package kvraft

import "time"

type Err string
type OpType string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrKilled      Err = "ErrKilled"
	ErrDuplicate   Err = "ErrDuplicate"
	ErrOutOfOrder  Err = "ErrOutOfOrder"
	ErrTimeout     Err = "ErrTimeout"
)

const (
	PutOp    string = "PutOp"
	AppendOp        = "AppendOp"
	GetOp           = "GetOp"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SeqId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId    int
}

type GetReply struct {
	Err   Err
	Value string
}

const ApplyTimeout = 10 * time.Second
