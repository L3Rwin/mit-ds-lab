package raft

// Raft API outline for services/tests:
//   rf = Make(...)
//   rf.Start(command) (index, term, isLeader)
//   rf.GetState() (term, isLeader)
// ApplyMsg is delivered on applyCh for committed entries or snapshots.

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"MIT6.824-6.5840/labgob"
	"MIT6.824-6.5840/labrpc"
)

// Each committed entry is sent to applyCh with CommandValid=true.
// For snapshots, use SnapshotValid=true and CommandValid=false.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int ``

	// For 2D (snapshots):
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Node state.
const Follower, Candidate, Leader int = 1, 2, 3
const tickInterval = 50 * time.Millisecond
const heartbeatTimeout = 150 * time.Millisecond

const snapshotInterval = 10

// Raft implements a single peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	appmu     sync.Mutex          // Lock to protect shared access to this peer's apply state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // channel to notify the service of committed entries

	// Persistent/volatile state (Figure 2 in the paper).
	state       int        // Follower/Candidate/Leader
	currentTerm int        // current term
	votedFor    int        // vote target
	logs        []ApplyMsg // log entries

	commitIndex      uint64        // highest committed index
	lastApplied      uint64        // highest applied index
	heartbeatTimeout time.Duration // heartbeat interval
	electionTimeout  time.Duration // election timeout
	lastElection     time.Time     // last election reset time
	lastHeartbeat    time.Time     // last heartbeat time
	peerTrackers     []PeerTracker // nextIndex/matchIndex per peer

	lastSnapshotIndex int
	lastSnapshotTerm  int
}
type RequestAppendEntriesArgs struct {
	LeaderTerm   int        // leader term
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of PrevLogIndex
	Logs         []ApplyMsg // log entries to store (may be empty)
	LeaderCommit uint64     // leader's commit index
}

type RequestAppendEntriesReply struct {
	FollowerTerm  int  // follower term for leader update
	Success       bool // whether append succeeded
	ConflictIndex int  // index of conflicting entry
	ConflictTerm  int  // term of conflicting entry
}

func (rf *Raft) getRelativeIndex(index int) int {
	return index - rf.lastSnapshotIndex - 1
}

func (rf *Raft) getLogLength() int {
	return len(rf.logs) + rf.lastSnapshotIndex
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.logs) > 0 {
		return rf.logs[len(rf.logs)-1].Term
	} else if rf.lastSnapshotIndex > 0 {
		return rf.lastSnapshotTerm
	}
	return 0
}

// GetState returns the current term and whether this peer is leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	////fmt.Printf("getting Leader State %d and term %d of node %d \n", rf.state, rf.currentTerm, rf.me)
	return term, isleader
}

// Persist Raft state to stable storage (Figure 2).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	raftstate := w.Bytes()
	// Keep existing snapshot when only persisting raft state.
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, voteFor, lastSnapshotIndex, lastSnapshotTerm int
	var logs []ApplyMsg

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		fmt.Println("decode failed")
		return
	}
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.logs = logs
	rf.lastSnapshotIndex = lastSnapshotIndex
	rf.lastSnapshotTerm = lastSnapshotTerm
	DPrintf("[%d][%d] Successfully ReadPersist with lastSnapshotIndex=%d and lastSnapshotTerm=%d, log length=%d", rf.me, rf.currentTerm, lastSnapshotIndex, lastSnapshotTerm, len(logs))
}

// RequestVote RPC arguments.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type RequestSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type RequestSnapshotReply struct {
	Term int
}

// Send a RequestVote RPC to a peer.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("ready to call RequestVote Method...")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestSnapshot(server int, args *RequestSnapshotArgs, reply *RequestSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Start appends a new command if this peer is leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index = rf.getLogLength() + 1
	term = rf.currentTerm

	rf.logs = append(rf.logs, ApplyMsg{CommandValid: true, Command: command, CommandIndex: index, Term: term})
	rf.persist()
	DPrintf("[%d][%d] leader started %d %d with commitIndex=%d, maxIdx=%d", rf.me, rf.currentTerm, index, command, rf.commitIndex, rf.getLogLength())
	return index, term, isLeader
}

// Kill marks this peer as dead; long-running loops should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Snapshot trims the log up to index and persists the snapshot.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastSnapshotIndex {
		return
	}
	rel := rf.getRelativeIndex(index)
	if rel < 0 || rel >= len(rf.logs) {
		return
	}
	rf.lastSnapshotTerm = rf.logs[rel].Term
	rf.logs = rf.logs[rel+1:]
	rf.lastSnapshotIndex = index
	rf.persistWithSnapshot(snapshot)
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

func (rf *Raft) StartAppendEntries(heart bool) {
	// Send heartbeats/entries to followers in parallel.
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.peerTrackers[i].nextIndex <= uint64(rf.lastSnapshotIndex) {
			go rf.SendSnapshot(i)
		} else {
			go rf.AppendEntries(i, heart)
		}

	}
}

func (rf *Raft) InstallSnapshot(args *RequestSnapshotArgs, reply *RequestSnapshotReply) {
	rf.mu.Lock()
	DPrintf("[%d][%d] Successfully InstallSnapshot", rf.me, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("[%d][%d] InstallSnapshot failed because leader term is less than follower term", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = None
	}
	reply.Term = rf.currentTerm
	rf.resetElectionTimer()

	rf.state = Follower

	if args.LastIncludedIndex <= rf.lastSnapshotIndex {
		rf.persist()
		rf.mu.Unlock()
		return
	}

	i := rf.getRelativeIndex(args.LastIncludedIndex)
	if i >= len(rf.logs) {
		rf.logs = rf.logs[:0]
	} else if i >= 0 {
		if rf.logs[i].Term != args.LastIncludedTerm {
			rf.logs = rf.logs[:0]
		} else {
			rf.logs = rf.logs[i+1:]
		}
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	if rf.commitIndex < uint64(args.LastIncludedIndex) {
		rf.commitIndex = uint64(args.LastIncludedIndex)
	}
	if rf.lastApplied < uint64(args.LastIncludedIndex) {
		rf.lastApplied = uint64(args.LastIncludedIndex)
	}
	rf.persistWithSnapshot(args.Data)
	rf.mu.Unlock()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.applyCh <- msg
}

// RequestAppendEntries handles heartbeats and log replication.
func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.LeaderTerm < rf.currentTerm {
		reply.FollowerTerm = rf.currentTerm
		reply.Success = false
		DPrintf("[%d][%d] RequestAppendEntries failed because leader term is less than follower term", rf.me, rf.currentTerm)

		return
	}
	if args.LeaderTerm > rf.currentTerm {
		rf.currentTerm = args.LeaderTerm
		rf.votedFor = None
	}
	reply.FollowerTerm = rf.currentTerm

	rf.resetElectionTimer()
	// 需要转变自己的身份为Follower
	rf.state = Follower

	j := rf.getRelativeIndex(int(args.PrevLogIndex))

	if int(args.PrevLogIndex) > rf.getLogLength() {
		reply.Success = false
		reply.ConflictIndex = rf.getLogLength() + 1
		reply.ConflictTerm = -1
		DPrintf("[%d][%d] RequestAppendEntries failed because PrevLogIndex is not consistent", rf.me, rf.currentTerm)
		DPrintf("[%d][%d] last log: %v vs %v", rf.me, rf.currentTerm, rf.getLogLength(), args.PrevLogIndex)

		return
	} else {
		var tgt int
		if j < 0 {
			tgt = rf.lastSnapshotTerm
		} else {
			tgt = rf.logs[j].Term
		}
		if tgt != int(args.PrevLogTerm) {
			reply.Success = false
			i := j - 1
			for i >= 0 && rf.logs[i].Term == tgt {
				i--
			}
			reply.ConflictIndex = i + rf.lastSnapshotIndex + 2
			reply.ConflictTerm = tgt
			DPrintf("[%d][%d] RequestAppendEntries failed because PrevLogTerm is not consistent", rf.me, rf.currentTerm)
			DPrintf("[%d][%d] last log: %v vs %v", rf.me, rf.currentTerm, tgt, args.PrevLogTerm)
			return
		}
	}

	j = 0
	if len(args.Logs) > 0 {

		i := int(args.PrevLogIndex) + 1

		// Skip matching prefix (same term).
		for j < len(args.Logs) && i <= rf.getLogLength() {
			if rf.logs[rf.getRelativeIndex(i)].Term != args.Logs[j].Term {
				// Truncate from i to end.
				rf.logs = rf.logs[:rf.getRelativeIndex(i)]
				DPrintf("[%d][%d] RequestAppendEntries logs truncated from %d to %d", rf.me, rf.currentTerm, i, rf.getLogLength())

				break
			}
			i++
			j++
		}

		// Append remaining entries.
	}

	if j < len(args.Logs) {
		rf.logs = append(rf.logs, args.Logs[j:]...)
	}
	if len(args.Logs) > 0 {
		DPrintf("[%d][%d] RequestAppendEntries logs appended from %d to %d", rf.me, rf.currentTerm, args.PrevLogIndex+1, rf.getLogLength())
		DPrintf("[%d][%d] last log: %v", rf.me, rf.currentTerm, rf.logs[len(rf.logs)-1])
	}

	// commitIndex only moves forward.
	newCommit := args.LeaderCommit
	lastIndex := uint64(rf.getLogLength())
	if newCommit > lastIndex {
		newCommit = lastIndex
	}
	if newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
	}

	reply.Success = true

}
func (rf *Raft) BuildSnapshotArg(args *RequestSnapshotArgs, targetServerId int) {
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastSnapshotIndex
	args.LastIncludedTerm = rf.lastSnapshotTerm
	args.Data = rf.persister.ReadSnapshot()

}

func (rf *Raft) SendSnapshot(targetServerId int) {
	reply := RequestSnapshotReply{}
	args := RequestSnapshotArgs{}
	rf.mu.Lock()
	rf.BuildSnapshotArg(&args, targetServerId)
	rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	ok := rf.sendRequestSnapshot(targetServerId, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = None
			rf.state = Follower
			rf.persist()
			return
		} else {
			newMatch := uint64(rf.lastSnapshotIndex)
			if newMatch > rf.peerTrackers[targetServerId].matchIndex {
				rf.peerTrackers[targetServerId].matchIndex = newMatch
				rf.peerTrackers[targetServerId].nextIndex = newMatch + 1
			}

			// Update commitIndex based on majority replication.
			rf.updateCommitIndex()

		}
	}
}

func (rf *Raft) BuildAEArg(args *RequestAppendEntriesArgs, targetServerId int) {
	args.LeaderTerm = rf.currentTerm
	// nextIndex is the next log index to send; PrevLogIndex is previous one.
	args.PrevLogIndex = rf.peerTrackers[targetServerId].nextIndex - 1

	if rf.getRelativeIndex(int(args.PrevLogIndex)) < 0 {
		args.PrevLogTerm = uint64(rf.lastSnapshotTerm)
	} else {
		args.PrevLogTerm = uint64(rf.logs[rf.getRelativeIndex(int(args.PrevLogIndex))].Term)
	}
	start := rf.getRelativeIndex(int(args.PrevLogIndex)) + 1
	if start < 0 {
		start = 0
	}
	if start < len(rf.logs) {
		logs := make([]ApplyMsg, len(rf.logs[start:]))
		copy(logs, rf.logs[start:])
		args.Logs = logs
	} else {
		args.Logs = nil
	}
	args.LeaderCommit = rf.commitIndex
}

// Update commitIndex by scanning from high to low; only current-term logs are committed.
func (rf *Raft) updateCommitIndex() {

	if rf.state != Leader {
		return
	}

	maxIndex := uint64(rf.getLogLength())

	for index := maxIndex; index > rf.commitIndex; index-- {
		if index <= uint64(rf.lastSnapshotIndex) {
			continue
		}
		count := 1 // Leader itself.
		for i := range rf.peers {
			if i != rf.me && rf.peerTrackers[i].matchIndex >= index {
				count++
			}
		}

		if 2*count > len(rf.peers) {

			logArrayIndex := int(index)
			if logArrayIndex <= rf.getLogLength() {
				rel := rf.getRelativeIndex(logArrayIndex)
				if rel >= 0 && rel < len(rf.logs) && rf.logs[rel].Term == rf.currentTerm {
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

	ok := rf.sendRequestAppendEntries(targetServerId, &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.FollowerTerm > rf.currentTerm {
			rf.currentTerm = reply.FollowerTerm
			rf.votedFor = None
			rf.state = Follower
			rf.persist()
			return
		} else if reply.Success == false {
			if reply.ConflictTerm == -1 {
				rf.peerTrackers[targetServerId].nextIndex = uint64(reply.ConflictIndex)
			} else {
				tgt := reply.ConflictTerm
				var i int
				for i = rf.getLogLength(); i > rf.lastSnapshotIndex; i-- {
					rel := rf.getRelativeIndex(int(i))
					if rf.logs[rel].Term == tgt {
						rf.peerTrackers[targetServerId].nextIndex = uint64(i) + 1
						break
					} else if rf.logs[rel].Term < tgt {
						rf.peerTrackers[targetServerId].nextIndex = uint64(reply.ConflictIndex)
						break
					}
				}
				if i == rf.lastSnapshotIndex {
					rf.peerTrackers[targetServerId].nextIndex = uint64(reply.ConflictIndex)
				}

			}
			if rf.peerTrackers[targetServerId].nextIndex <= uint64(rf.lastSnapshotIndex) {
				rf.peerTrackers[targetServerId].nextIndex = uint64(rf.lastSnapshotIndex)
			}
		} else {
			newMatch := args.PrevLogIndex + uint64(len(args.Logs))
			if newMatch > rf.peerTrackers[targetServerId].matchIndex {
				rf.peerTrackers[targetServerId].matchIndex = newMatch
				rf.peerTrackers[targetServerId].nextIndex = newMatch + 1
			}
			// Update commitIndex after successful replication.
			rf.updateCommitIndex()
		}
	}

}

func (rf *Raft) SayMeL() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return fmt.Sprintf("[Server %v as %v at term %v]", rf.me, rf.state, rf.currentTerm)
}

// Make creates a Raft peer and starts background goroutines.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = None
	rf.state = Follower
	rf.resetElectionTimer()
	rf.logs = make([]ApplyMsg, 0)
	rf.heartbeatTimeout = heartbeatTimeout
	// Initialize peer trackers.
	rf.peerTrackers = make([]PeerTracker, len(peers))
	for i := range rf.peerTrackers {
		rf.peerTrackers[i].nextIndex = uint64(rf.getLogLength() + 1)
		rf.peerTrackers[i].matchIndex = 0
	}

	// Restore persisted state.
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0

	rf.readPersist(rf.persister.ReadRaftState())

	DPrintf("[%d][%d] Successfully Start", rf.me, rf.currentTerm)
	// Background loops.
	go rf.ticker()
	go rf.applier()
	return rf
}

func (rf *Raft) ticker() {
	// Stay alive until killed.
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

// Send newly committed entries to applyCh in order.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.tryApplyEntries()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) tryApplyEntries() {
	var msgs []ApplyMsg
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		relIdx := rf.getRelativeIndex(int(rf.lastApplied))
		if relIdx < 0 || relIdx >= len(rf.logs) {
			continue
		}
		entry := rf.logs[relIdx]
		msgs = append(msgs, ApplyMsg{
			CommandValid: entry.CommandValid,
			Command:      entry.Command,
			CommandIndex: int(rf.lastApplied),
			Term:         entry.Term,
		})
	}
	rf.mu.Unlock()
	for _, msg := range msgs {
		DPrintf("[%d][%d] applying log entry %d", rf.me, msg.Term, msg.CommandIndex)
		rf.applyCh <- msg
	}
}
