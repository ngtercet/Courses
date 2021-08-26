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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	// limited to 10 heartbeats per second
	heartbeatInterval  = 100
	electionTimeoutMin = 200
	electionTimeoutMax = 350
	FOLLOWER           = 1
	CANDIDATE          = FOLLOWER << 1
	LEADER             = CANDIDATE << 1
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	role      int32

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.``

	// persistent state on all servers
	currentTerm int
	voteFor     int
	log         []*LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// set to time.Now() when:
	// 1. receive AppendEntries RPC from current leader
	// 2. grant vote to candidate
	heartbeatTime time.Time
}

type LogEntry struct {
	Term int
	Cmd  string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A: uses term, candidateId in args
	// 2B: TODO with lastLogIndex, lastLogTerm
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[RequestVote REQ] peer %d term %d -> peer %d term %d role %d", args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.role)

	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}

	rf.becomeFollowerIfNeeded(args.Term)

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && args.LastLogIndex >= len(rf.log)-1) {
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			DPrintf("peer %d vote for %d", rf.me, args.CandidateId)
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.resetTimeOut()
			return
		}
	}

	reply.VoteGranted = false
	DPrintf("peer %d reject vote for %d, voteFor is %d", rf.me, args.CandidateId, rf.voteFor)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A: uses term, leaderId
	// 2B: TODO with prevLogIndex, prevLogTerm, entrites, leaderCommit

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("peer %d receives AppendEntries", rf.me)

	// Reply false if term < currentTerm (§5.1)
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}

	rf.becomeFollowerIfNeeded(args.Term)
	rf.resetTimeOut()

	// TODO Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)

	// TODO If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)

	// TODO Append any new entries not already in the log

	// TODO If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() && rf.getRole() != LEADER {

		// DPrintf("peer %d tick, role: %d", rf.me, rf.getRole())

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		timeToSleep := randomTimeout(electionTimeoutMin, electionTimeoutMax)
		time.Sleep(time.Duration(timeToSleep) * time.Millisecond)

		if rf.getRole() != LEADER && int(time.Since(rf.getHeartbeatTime()).Milliseconds()) > timeToSleep {
			rf.startElection()
		}
	}
}

func randomTimeout(min, max int) int {
	return min + int(rand.Intn(max-min))
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	DPrintf("peer: %d becomes FOLLOWER", me)

	rf.become(FOLLOWER)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// =============utils==================

func (rf *Raft) startElection() {
	DPrintf("[startElection] peer %d timeout", rf.me)

	rf.mu.Lock()
	DPrintf("peer %d becomes CANDIDATE", rf.me)
	rf.become(CANDIDATE)
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex < 0 {
		lastLogTerm = -1
	} else {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	votes := 1
	cond := sync.NewCond(&rf.mu)
	cnt := 0

	for idx, peer := range rf.peers {

		if idx != rf.me {
			go func(idx int, peer *labrpc.ClientEnd) {
				var reply RequestVoteReply
				peer.Call("Raft.RequestVote", &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// check term in reply
				rf.becomeFollowerIfNeeded(reply.Term)

				cnt += 1
				DPrintf("[RequestVote RSP] peer %d term %d vote granted: %t -> peer %d term %d role %d", idx, reply.Term, reply.VoteGranted, rf.me, rf.currentTerm, rf.role)
				if rf.role == CANDIDATE && reply.VoteGranted {
					votes += 1
					if votes*2 > len(rf.peers) {
						cond.Broadcast()
					}
				} else {
					if cnt == len(rf.peers)-1 {
						cond.Broadcast()
					}
				}
			}(idx, peer)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	cond.Wait()
	if votes*2 > len(rf.peers) {
		DPrintf("peer %d becomes LEADER", rf.me)
		rf.become(LEADER)
	}
}

func (rf *Raft) heartbeatBG() {

	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	reply := AppendEntriesReply{}

	for !rf.killed() && rf.getRole() == LEADER {
		for idx, peer := range rf.peers {
			if idx != rf.me {
				go func(idx int, peer *labrpc.ClientEnd) {
					// DPrintf("peer %d send heartbeat to peer %d", rf.me, idx)
					a := peer.Call("Raft.AppendEntries", &args, &reply)
					if a {
						rf.mu.Lock()
						rf.becomeFollowerIfNeeded(reply.Term)
						rf.mu.Unlock()
					}
				}(idx, peer)
			}
		}
		time.Sleep(heartbeatInterval)
	}

	// DPrintf("peer %d ends heartbeat, killed: %t, role: %t", rf.me, rf.killed(), rf.role == LEADER)
}

func (rf *Raft) getRole() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.role)
}

func (rf *Raft) getHeartbeatTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.heartbeatTime
}

func (rf *Raft) become(role int32) {
	rf.role = role

	switch role {
	case LEADER:
		go rf.heartbeatBG()
		rf.nextIndex = make([]int, len(rf.peers)-1)
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log) + 1
		}
		rf.matchIndex = make([]int, len(rf.peers)-1)
	case FOLLOWER:
		rf.voteFor = -1
		rf.resetTimeOut()
		rf.nextIndex = nil
		rf.matchIndex = nil
	case CANDIDATE:
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.resetTimeOut()
		rf.nextIndex = nil
		rf.matchIndex = nil
	default:
	}
}

func (rf *Raft) resetTimeOut() {
	rf.heartbeatTime = time.Now()
	// DPrintf("peer %d reset timeout", rf.me)
}

func (rf *Raft) becomeFollowerIfNeeded(term int) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		if rf.role != FOLLOWER {
			DPrintf("peer %d becomes FOLLOWER", rf.me)
			rf.become(FOLLOWER)
		} else {
			rf.voteFor = -1
		}
	}
}
