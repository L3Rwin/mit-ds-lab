package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"MIT6.824-6.5840/labgob"
)

// Debugging
const Debug_level = 1

func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	if Debug_level <= level {
		log.Printf(format, a...)
	}
	return
}
func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func ifCond(cond bool, a, b interface{}) interface{} {
	if cond {
		return a
	} else {
		return b
	}
}
func getcount() func() int {
	cnt := 1000
	return func() int {
		cnt++
		return cnt
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func register() {
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

}
