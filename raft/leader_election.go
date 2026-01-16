package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// Base election timeout is T; actual timeout is in [T, 2T).
const baseElectionTimeout = 300
const None = -1

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	term := rf.currentTerm
	me := rf.me
	lastLogIndex := rf.getLogLength()
	lastLogTerm := rf.getLastLogTerm()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	done := false
	votes := 1
	fmt.Printf("[%d] attempting an election at term %d...", me, term)

	for i := range rf.peers {
		if me == i {
			continue
		}
		// Ask this peer for a vote.
		go func(serverId int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok {
				DPrintf("%v: cannot give a Vote to %v args.term=%v\n", rf.SayMeL(), serverId, args.Term)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Tally votes.
			if reply.VoteGranted {
				votes++
			} else if reply.Term > term {
				rf.currentTerm = reply.Term
				rf.votedFor = None
				rf.state = Follower
				rf.persist()
				return
			}

			if done || votes <= len(rf.peers)/2 {
				return
			}
			if rf.state != Candidate {
				return
			}
			fmt.Printf("\n[%d] got enough votes, and now is the leader (currentTerm=%d, maxIdx = %d)!\n", rf.me, rf.currentTerm, rf.getLogLength())
			fmt.Printf("[%d] All log entries: ", rf.me)
			for idx, entry := range rf.logs {
				fmt.Printf("idx=%d term=%d cmd=%v ", idx, entry.Term, entry.Command)
			}
			fmt.Printf("\n")
			done = true
			rf.state = Leader
			// Reset peer trackers.
			for i := range rf.peerTrackers {
				rf.peerTrackers[i].nextIndex = uint64(rf.getLogLength()) + 1
				rf.peerTrackers[i].matchIndex = 0
			}
			rf.StartAppendEntries(true) // Send an immediate heartbeat.
		}(i)
	}
}

func (rf *Raft) pastElectionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionTimeout
}

func (rf *Raft) resetElectionTimer() {
	electionTimeout := baseElectionTimeout + (rand.Int63() % baseElectionTimeout)
	rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) becomeFollower(term int) bool {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = None
		return true
	}
	return false
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
}

// RequestVote handles incoming vote requests.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	// Reject if candidate term is stale.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = None
		rf.state = Follower
	}

	logLastIndex := rf.getLogLength()
	logLastTerm := rf.getLastLogTerm()
	update := args.LastLogTerm > logLastTerm || (args.LastLogIndex >= logLastIndex && args.LastLogTerm == logLastTerm)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.resetElectionTimer()
		DPrintf("[%d][%d] Granted vote to %d at term %d", rf.me, rf.currentTerm, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}
