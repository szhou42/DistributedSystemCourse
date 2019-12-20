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
	"sync"
	"time"
	"math/rand"
	"labrpc"
)

/*
Concurrent code

1 AppendEntries handler
2 RequestVote handler
2 Sending Heartbeats
3 Electing self

*/


// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command string
	Term int	// When does leader received this term
}


const FOLLOWER int = 0
const LEADER int = 1
const CANDIDATE int = 2

const HEARTBEAT_FREQUNCY time.Duration = 400 * time.Millisecond
const ELECTION_TIMEOUT_LOWER = 300
const ELECTION_TIMEOUT_UPPER = 600

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	serverRole int
	electionTimer *time.Timer

	currentTerm int
	votedFor int
	logs []Log

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int


	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	serverRole := rf.serverRole
	rf.mu.Unlock()
	return currentTerm, serverRole == LEADER
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// AppendEntries RPC arguments structure, for heartbeat and appending entries
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PrevLogIndex int
	PrevLogTerm int
	LogEntries[] Log
	LeaderCommitIndex int
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) Elect() {
	DPrintf("s%d Electing...\n", rf.me)
	// The server has not received communication from leaders/candidates for a while,
	// in this case it should become a candidate itself and request votes from others
	var mutex sync.Mutex

	var votesLock sync.Mutex

	condvar := sync.NewCond(&mutex)


	rf.mu.Lock()
	if rf.serverRole == FOLLOWER {
		// Request votes
		rf.serverRole = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = -1
		votesCount := 1
		rf.mu.Unlock()

		DPrintf("s%d Electing in term %d...\n", rf.me, rf.currentTerm)
		DPrintf("s%d Requesting votes...\n", rf.me)

		finished := false
		for i, _ := range rf.peers {
			if i == rf.me {
				// No need to send vote request to itself
				continue
			}
			go func(index int) {
				var requestVoteReply RequestVoteReply
				var lastLogIndex int
				var lastLogTerm int
				var currentTerm int

				rf.mu.Lock()
				lastLogIndex = len(rf.logs)
				if lastLogIndex > 0 {
					lastLogTerm = rf.logs[lastLogIndex - 1].Term
				}
				currentTerm = rf.currentTerm
				rf.mu.Unlock()

				rf.sendRequestVote(index, &RequestVoteArgs{
					Term:         currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}, &requestVoteReply)

				rf.mu.Lock()
				// Check if current term is outdated, also if a leader is discovered
				if requestVoteReply.Term > rf.currentTerm {
					rf.currentTerm = requestVoteReply.Term
					rf.serverRole = FOLLOWER
					rf.votedFor = -1
				}
				votesLock.Lock()
				if requestVoteReply.VoteGranted {
					votesCount += 1
				}

				if votesCount > len(rf.peers) / 2 {
					condvar.Signal()
				}
				votesLock.Unlock()
				rf.mu.Unlock()
			}(i)
		}
		// Wait until all votes are collected
		time.AfterFunc(300 * time.Millisecond, func() {
			votesLock.Lock()
			finished = true
			votesLock.Unlock()
			condvar.Signal()
		})
		condvar.L.Lock()
		for {
			rf.mu.Lock()
			numPeers := len(rf.peers)
			rf.mu.Unlock()

			votesLock.Lock()
			votesCountClone := votesCount

			if votesCountClone > numPeers / 2 || finished {
				votesLock.Unlock()
				break
			}
			votesLock.Unlock()
			condvar.Wait()
		}
		condvar.L.Unlock()

		rf.mu.Lock()
		// Am I still a candidate or have I already been converted to a follower? Do I have enough votes to be leader?
		if rf.serverRole == CANDIDATE && votesCount > len(rf.peers) / 2 {
			rf.serverRole = LEADER
			DPrintf("s%d elected as leader with %d votes, term:%d\n", rf.me, votesCount, rf.currentTerm)
		} else {
			DPrintf("s%d not elected,votes:%d, term:%d\n", rf.me, votesCount, rf.currentTerm)
			rf.serverRole = FOLLOWER
		}
		rf.resetElectionTimer()
		rf.mu.Unlock()
	} else {
		rf.resetElectionTimer()
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("s%d Got vote request from %d\n", rf.me, args.CandidateID)

	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm

	reason := "None"
	rf.mu.Lock()
	if rf.serverRole == CANDIDATE && rf.currentTerm == args.Term {
		reason = "I voted for myself"
		reply.VoteGranted = false
	} else if args.Term < rf.currentTerm {
		reason = "Candidate's term is smaller than mine"
		reply.VoteGranted = false
	} else if args.Term >= rf.currentTerm {
		// Make sure to update current term and switch to follower, otherwise this server may still think of itself as a leader
		rf.currentTerm = args.Term
		rf.serverRole = FOLLOWER

		// Decide if we should vote for the candidate based on votedFor and logs
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			// Check if candidate's logs is at least as up-to-date as mine
			if len(rf.logs) == 0 {
				reply.VoteGranted = true
				reason = "I have no logs"
			} else {
				if rf.logs[len(rf.logs) - 1].Term != args.LastLogTerm {
					reply.VoteGranted = args.LastLogTerm > rf.logs[len(rf.logs) - 1].Term
					reason = "Comparing log term"
				} else {
					reply.VoteGranted = args.LastLogIndex >= len(rf.logs)
					reason = "Comparing log index"
				}
			}
		}
	}

	if reply.VoteGranted {
		DPrintf("s%d voted for %d, term:%d, reason:%s\n", rf.me, args.CandidateID, rf.currentTerm, reason)
		rf.votedFor = args.CandidateID
		rf.resetElectionTimer()
	} else {
		DPrintf("s%d did not vote for %d, term:%d, reason:%s\n", rf.me, args.CandidateID, rf.currentTerm, reason)
	}
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.serverRole = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	DPrintf("s%d Starting...", rf.me)
	// Periodically send heartbeats
	go rf.startHeartbeats()

	// Set a timer that wakes up every 150-300 ms, if it ever really wakes up, it means it hasn't received communication from
	// leaders/candidates for a while, it this case it should become a candidate itself and request votes from others
	rf.electionTimer = time.AfterFunc(rf.genRandomElectionTimeout(), rf.Elect)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset((time.Duration(300 + rand.Intn(600 + 1))) * time.Millisecond)
}

func (rf *Raft) sendSingleHeartbeat(index int) {
	var appendEntriesReply AppendEntriesReply

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	rf.sendAppendEntries(index, &AppendEntriesArgs{
		LogEntries:[]Log{},
		Term:currentTerm,
	}, &appendEntriesReply)

	rf.mu.Lock()
	if appendEntriesReply.Term > rf.currentTerm {
		rf.currentTerm = appendEntriesReply.Term
		rf.serverRole = FOLLOWER
		rf.votedFor = -1
	}
	rf.mu.Unlock()
}

func (rf *Raft) startHeartbeats() {
	for {

		_, isLeader := rf.GetState()
		if isLeader	{
			DPrintf("s%d Sending heartbeats.. isLeader:%v\n", rf.me, isLeader)
			for i, _ := range rf.peers {
				// No need to send heartbeat to leader itself
				if i == rf.me {
					continue
				}
				go rf.sendSingleHeartbeat(i)
			}
		}
		time.Sleep(HEARTBEAT_FREQUNCY)
	}
}

func (rf *Raft) genRandomElectionTimeout() time.Duration {
	return time.Duration(ELECTION_TIMEOUT_LOWER + rand.Intn(ELECTION_TIMEOUT_UPPER + 1)) * time.Millisecond
}


//
// Append Entries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	rf.resetElectionTimer()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	//  If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if rf.logs[args.PrevLogIndex].Term != args.Term {
		rf.logs = rf.logs[:args.PrevLogIndex]
	}

	// Append any new entries not already in the log
	for _, log := range args.LogEntries {
		rf.logs = append(rf.logs, log)
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommitIndex > rf.commitIndex {
		if args.LeaderCommitIndex < len(rf.logs) - 1 {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = len(rf.logs) - 1
		}
	}

	// For now, as long as leader is valid, we always return true, will need to change this later
	reply.Success = true

	// When we're using a higher term number from leader, we want to reset votedFor because we haven't voted at this level yet
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
	}

	// Candidate/Invalid Server will switch to follower when discovering a leader(by seeing this heartbeat)
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.serverRole = FOLLOWER
	}
	DPrintf("s%d Receive heartbeat in term:%d\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	// Append the log here?
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSingleAppendEntries(server int, command Log) bool {
	var appendEntriesArg AppendEntriesArgs
	var appendEntriesReply AppendEntriesReply

	// Construct appendEntriesArg here
	appendEntriesArg.Term = rf.currentTerm
	appendEntriesArg.LeaderID = rf.me

	ok := rf.sendAppendEntries(server, &appendEntriesArg, &appendEntriesReply)
	for !ok {
		ok = rf.sendAppendEntries(server, &appendEntriesArg, &appendEntriesReply)
	}

	// Handle reply here
	if appendEntriesReply.Term > rf.currentTerm {
		rf.serverRole = FOLLOWER
		rf.currentTerm = appendEntriesArg.Term
		rf.votedFor = -1
	}

	return appendEntriesReply.Success
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	term, isLeader = rf.GetState()
	index = len(rf.logs)

	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	rf.logs = append(rf.logs, command.(Log))

	rf.mu.Unlock()

	for i, _ := range rf.peers {
		// No need to send AppendEntries to leader itself
		if i == rf.me {
			continue
		}
		go rf.sendSingleAppendEntries(i, command.(Log))
	}

	return index, term, isLeader
}