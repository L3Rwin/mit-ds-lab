package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"MIT6.824-6.5840/labgob"
	"MIT6.824-6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int ``

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 设置状态类型
const Follower, Candidate, Leader int = 1, 2, 3
const tickInterval = 50 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	appmu     sync.Mutex          // Lock to protect shared access to this peer's apply state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // channel to notify the service of committed entries

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int        // 节点状态，Candidate-Follower-Leader
	currentTerm int        // 当前的任期
	votedFor    int        // 投票给谁
	logs        []ApplyMsg // 日志

	commitIndex      uint64        // 已提交的最高的日志项目的索引
	lastApplied      uint64        // 最后一个应用的日志项目的索引
	heartbeatTimeout time.Duration // 心跳定时器
	electionTimeout  time.Duration //选举计时器
	lastElection     time.Time     // 上一次的选举时间，用于配合since方法计算当前的选举时间是否超时
	lastHeartbeat    time.Time     // 上一次的心跳时间，用于配合since方法计算当前的心跳时间是否超时
	peerTrackers     []PeerTracker // keeps track of each peer's next index, match index, etc.

}
type RequestAppendEntriesArgs struct {
	LeaderTerm   int        // Leader的Term
	PrevLogIndex uint64     // 新日志条目的上一个日志的索引
	PrevLogTerm  uint64     // 新日志的上一个日志的任期
	Logs         []ApplyMsg // 需要被保存的日志条目,可能有多个
	LeaderCommit uint64     // Leader已提交的最高的日志项目的索引
}

type RequestAppendEntriesReply struct {
	FollowerTerm  int  // Follower的Term,给Leader更新自己的Term
	Success       bool // 是否推送成功
	ConflictIndex int  // 冲突的条目的下标
	ConflictTerm  int  // 冲突的条目的任期
}

func (rf *Raft) getHeartbeatTime() time.Duration {
	return time.Millisecond * 110
}

// #region agent log

// #endregion

// 随机化的选举超时时间
func (rf *Raft) getElectionTime() time.Duration {
	// [250,400) 250+[0,150]
	// return time.Millisecond * time.Duration(250+15*rf.me)
	//return time.Millisecond * time.Duration(350+rand.Intn(1000))

	return time.Millisecond * time.Duration(350+rand.Intn(200))
}

// 这个是只给tester调的
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	////fmt.Printf("getting Leader State %d and term %d of node %d \n", rf.state, rf.currentTerm, rf.me)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.mu.Lock()
	defer rf.persister.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	rf.persister.raftstate = w.Bytes()
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.persister.mu.Lock()
	defer rf.persister.mu.Unlock()
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, voteFor int
	var logs []ApplyMsg

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		fmt.Println("decode failed")
		return
	}
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.logs = logs
	rf.persister.raftstate = data

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("ready to call RequestVote Method...")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index = len(rf.logs)
	term = rf.currentTerm

	rf.logs = append(rf.logs, ApplyMsg{CommandValid: true, Command: command, CommandIndex: index, Term: term})
	rf.persist()
	DPrintf("[%d][%d] leader started %d %d with commitIndex=%d, maxIdx=%d", rf.me, rf.currentTerm, index, command, rf.commitIndex, len(rf.logs)-1)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead) // 这里的kill仅仅将对应的字段置为1
	return z == 1
}

func (rf *Raft) callRequestVote(serverId int) bool {
	//log.Printf("[%d] ready to send request vote to %d", rf.me, serverId)
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	var reply RequestVoteReply
	ok := rf.sendRequestVote(serverId, &args, &reply)
	//log.Printf("[%d] finish sending request vote to %d", rf.me, serverId)
	if !ok {
		return false
	}
	return true
}

// // the service says it has created a snapshot that has
// // all info up to and including index. this means the
// // service no longer needs the log through (and including)
// // that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) StartAppendEntries(heart bool) {
	// rf.peerTrackers[rf.me].nextIndex += 1

	// 并行向其他节点发送心跳，让他们知道此刻已经有一个leader产生
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.AppendEntries(i, heart)
	}
}

// 定义一个心跳兼日志同步处理器，这个方法是Candidate和Follower节点的处理
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// #region agent log

	// #endregion
	rf.mu.Lock() // 加接收心跳方的锁
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		DPrintf("[%d][%d] RequestAppendEntries failed because leader term is less than follower term", rf.me, rf.currentTerm)

		return
	}
	rf.currentTerm = args.LeaderTerm
	reply.FollowerTerm = args.LeaderTerm

	rf.resetElectionTimer()
	// 需要转变自己的身份为Follower
	rf.state = Follower
	if args.LeaderTerm > rf.currentTerm {

		rf.votedFor = None
	}

	// log index从0开始，所以logs[i]对应log index i
	// 由于Make时预载了index=0的entry，PrevLogIndex应该>=0
	if int(args.PrevLogIndex) >= len(rf.logs) {
		// #region agent log

		// #endregion
		reply.Success = false
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
		DPrintf("[%d][%d] RequestAppendEntries failed because PrevLogIndex is not consistent", rf.me, rf.currentTerm)
		DPrintf("[%d][%d] last log: %v vs %v", rf.me, rf.currentTerm, len(rf.logs)-1, args.PrevLogIndex)
		// DPrintf("[%d][%d] local prev log term: %v, args.PrevLogTerm: %v", rf.me, rf.currentTerm, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	} else if rf.logs[args.PrevLogIndex].Term != int(args.PrevLogTerm) {
		reply.Success = false
		i := int(args.PrevLogIndex) - 1
		tgt := rf.logs[args.PrevLogIndex].Term
		for i > 0 && rf.logs[i].Term == tgt {
			i--
		}
		reply.ConflictIndex = i + 1
		reply.ConflictTerm = tgt
		DPrintf("[%d][%d] RequestAppendEntries failed because PrevLogTerm is not consistent", rf.me, rf.currentTerm)
		DPrintf("[%d][%d] last log: %v vs %v", rf.me, rf.currentTerm, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	j := 0
	if len(args.Logs) > 0 {
		// firstNew := PrevLogIndex + 1
		firstNew := int(args.PrevLogIndex) + 1
		i := firstNew

		lastLogIndex := len(rf.logs) - 1

		// 先跳过已经相同的部分（term 一致就继续向后比较）
		for j < len(args.Logs) && i <= lastLogIndex {
			if rf.logs[i].Term != args.Logs[j].Term {
				// truncateFrom(i)：删除 i..end
				rf.logs = rf.logs[:i]
				DPrintf("[%d][%d] RequestAppendEntries logs truncated from %d to %d", rf.me, rf.currentTerm, i, len(rf.logs)-1)
				lastLogIndex = i - 1
				break
			}
			i++
			j++
		}

		// 追加还没存在的部分

		DPrintf("[%d][%d] RequestAppendEntries logs appended from %d to %d", rf.me, rf.currentTerm, args.PrevLogIndex+1, len(rf.logs)-1)
		DPrintf("[%d][%d] last log: %v", rf.me, rf.currentTerm, rf.logs[len(rf.logs)-1])
		// #region agent log

		// #endregion
	}

	if j < len(args.Logs) {
		rf.logs = append(rf.logs, args.Logs[j:]...)
	}

	// commitIndex 只能增加，不能减少
	// after log consistency + possible append
	newCommit := args.LeaderCommit
	lastIndex := uint64(len(rf.logs) - 1)
	if newCommit > lastIndex {
		newCommit = lastIndex
	}
	if newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
	}

	// #region agent log
	// #endregion
	// #region agent log

	reply.Success = true

	// commitIndex 可能推进，唤醒应用协程
	// rf.mu.Unlock()
	// rf.tryApplyEntries()
	// rf.mu.Lock()

	//fmt.Printf("\n  %d receive heartbeat at leader's term %d, and my term is %d", rf.me, args.LeaderTerm, rf.currentTerm)

	// 重置自身的选举定时器，这样自己就不会重新发出选举需求（因为它在ticker函数中被阻塞住了）
	//log.Printf("[%v]'s electionTimeout is reset and its state converts to %v", rf.me, rf.state)
}

func (rf *Raft) BuildAEArg(args *RequestAppendEntriesArgs, targetServerId int) {
	args.LeaderTerm = rf.currentTerm
	// nextIndex表示下一个要发送的log index，PrevLogIndex是前一个log index
	// 由于Make时预载了index=0的entry，nextIndex应该>=1
	// 如果nextIndex=0，修复为1（因为logs至少有1个entry at index 0）
	nextIndex := rf.peerTrackers[targetServerId].nextIndex
	if nextIndex == 0 {
		nextIndex = 1
	}
	args.PrevLogIndex = nextIndex - 1
	args.PrevLogTerm = uint64(rf.logs[args.PrevLogIndex].Term)
	start := int(args.PrevLogIndex) + 1
	if start < len(rf.logs) {
		logs := make([]ApplyMsg, len(rf.logs[start:]))
		copy(logs, rf.logs[start:])
		args.Logs = logs
	} else {
		args.Logs = nil
	}
	args.LeaderCommit = rf.commitIndex
}

// 更新 commitIndex：从高到低扫描，找到大多数节点都复制的最高 index
// 只能提交当前任期的日志（或者之前任期的日志如果已经被提交）
func (rf *Raft) updateCommitIndex() {
	// #region agent log

	// #endregion
	if rf.state != Leader {
		return
	}

	maxIndex := uint64(len(rf.logs) - 1)

	for index := maxIndex; index > rf.commitIndex; index-- {
		count := 1 // Leader 自己（Leader 总是有所有日志）
		for i := range rf.peers {
			if i != rf.me && rf.peerTrackers[i].matchIndex >= index {
				count++
			}
		}

		// 如果大多数节点都复制了该 index
		if 2*count > len(rf.peers) {

			logArrayIndex := int(index)
			if logArrayIndex >= 0 && logArrayIndex < len(rf.logs) {
				// #region agent log

				// #endregion
				if rf.logs[logArrayIndex].Term == rf.currentTerm {

					rf.commitIndex = index

					break
				}
			}
		}
	}
}

func (rf *Raft) AppendEntries(targetServerId int, heart bool) {
	reply := RequestAppendEntriesReply{}
	args := RequestAppendEntriesArgs{}
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.BuildAEArg(&args, targetServerId)
	rf.mu.Unlock()

	// log append

	ok := rf.sendRequestAppendEntries(targetServerId, &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.peerTrackers[targetServerId].lastAck = time.Now()
		if reply.FollowerTerm > rf.currentTerm {
			rf.currentTerm = reply.FollowerTerm
			rf.votedFor = None
			rf.state = Follower
			rf.persist()
			return
		} else if reply.Success == false {
			// #region agent log

			// #endregion
			if reply.ConflictTerm == -1 {
				rf.peerTrackers[targetServerId].nextIndex = uint64(reply.ConflictIndex)
			} else {
				tgt := reply.ConflictTerm
				var i int
				for i := len(rf.logs) - 1; i > 0; i-- {
					if rf.logs[i].Term == tgt {
						rf.peerTrackers[targetServerId].nextIndex = uint64(i) + 1
						break
					} else if rf.logs[i].Term < tgt {
						rf.peerTrackers[targetServerId].nextIndex = uint64(reply.ConflictIndex)
						break
					}
				}
				if i == 0 {
					rf.peerTrackers[targetServerId].nextIndex = uint64(reply.ConflictIndex)
				}

			}
			// #region agent log

			// #endregion
		} else {
			newMatch := args.PrevLogIndex + uint64(len(args.Logs))
			// #region agent log

			// #endregion
			if newMatch > rf.peerTrackers[targetServerId].matchIndex {
				rf.peerTrackers[targetServerId].matchIndex = newMatch
				rf.peerTrackers[targetServerId].nextIndex = newMatch + 1
			}
			// 更新 commitIndex：从高到低扫描，找到大多数节点都复制的最高 index
			rf.updateCommitIndex()
			// rf.mu.Unlock()
			// rf.tryApplyEntries()
			// rf.mu.Lock()
		}
	}

}

func (rf *Raft) SayMeL() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower //设置节点的初始状态为follower
	rf.resetElectionTimer()
	rf.logs = make([]ApplyMsg, 1)
	rf.logs[0] = ApplyMsg{CommandValid: true, Command: nil, CommandIndex: 0, Term: 0}
	rf.heartbeatTimeout = heartbeatTimeout // 这个是固定的
	// 初始化 peerTrackers，nextIndex 初始化为 len(logs) = 1（因为预载了 index=0 的 entry）
	rf.peerTrackers = make([]PeerTracker, len(peers))
	for i := range rf.peerTrackers {
		rf.peerTrackers[i].nextIndex = uint64(len(rf.logs)) // 初始化为 1
		rf.peerTrackers[i].matchIndex = 0                   // 初始化为 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.persister.RaftStateSize() == 0 {
		rf.persist()
	}
	//Leader选举协程
	//fmt.Printf("finishing creating raft node %d", rf.me)
	go rf.ticker()
	go rf.applier()
	return rf
}

func (rf *Raft) ticker() {
	// 如果这个raft节点没有掉线,则一直保持活跃不下线状态（可以因为网络原因掉线，也可以tester主动让其掉线以便测试）
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		shouldElection := (state == Follower || state == Candidate) && rf.pastElectionTimeout()
		shouldHeartbeat := state == Leader && rf.pastHeartbeatTimeout()
		if shouldHeartbeat {
			rf.resetHeartbeatTimer()
		}
		rf.mu.Unlock()

		if shouldElection {
			rf.StartElection()
		} else if state == Leader {
			rf.StartAppendEntries(shouldHeartbeat)
		}
		time.Sleep(tickInterval)
	}
	fmt.Printf("tim")
}

func (rf *Raft) SaySth() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
}

// 将 commitIndex 之后的日志按顺序发送到 applyCh
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.tryApplyEntries()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) tryApplyEntries() {
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		idx := rf.lastApplied
		if int(idx) >= len(rf.logs) {
			// 防御：日志不完整，不发送
			continue
		}
		entry := rf.logs[idx]
		msg := ApplyMsg{
			CommandValid: entry.CommandValid,
			Command:      entry.Command,
			CommandIndex: int(idx),
			Term:         entry.Term,
		}
		// #region agent log

		// #endregion
		// rf.appmu.Unlock()
		DPrintf("[%d][%d] applying log entry %d", rf.me, rf.currentTerm, idx)
		rf.applyCh <- msg
		// rf.appmu.Lock()
	}
	rf.mu.Unlock()
}
