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

	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverState int

const (
	followerState serverState = 1 << iota
	candidateState
	leaderState
)

func (state *serverState) stateStr() string {
	if *state == followerState {
		return "Follower"
	}
	if *state == candidateState {
		return "Candidate"
	}
	return "Leader"
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	// unit: ms
	heartbeatPeriod               int64 = 110
	electionTimeoutBase           int64 = 700
	electionTimeoutVariationRange int64 = 100
)

func heartbeatTimeout() int64 {
	return heartbeatPeriod
}

func electionTimeout() int64 {
	return electionTimeoutBase + (rand.Int63() % electionTimeoutVariationRange)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	timer    int64
	leaderId int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	state       serverState
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (-1 if none)
	log         []LogEntry // log entries
	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// volatile state on leaders (reinitialized after election)
	nextIndex  []int // index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	currentTerm, currentState := rf.raftState()
	return currentTerm, currentState == leaderState
}

func (rf *Raft) raftState() (int, serverState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	state := rf.state
	return term, state
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (rf *Raft) initRequestVoteArgs() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log) - 1
	if args.LastLogIndex == -1 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	return &args
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term    int
	Granted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	Log(rf.me, "Receive vote request from "+strconv.Itoa(args.CandidateId)+".")
	reply.Term = rf.currentTerm
	reply.Granted = false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// deny any requests that term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = followerState
		rf.votedFor = -1
		rf.timer = electionTimeout()
	}
	// If votedFor is null or candidateId, and candidate’s log is
	// at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		logLength := len(rf.log)
		if logLength == 0 {
			reply.Granted = true
		} else if rf.log[logLength-1].Term < args.LastLogTerm {
			reply.Granted = true
		} else if rf.log[logLength-1].Term == args.LastLogTerm {
			reply.Granted = (logLength-1 <= args.LastLogIndex)
		}
	}

	if reply.Granted {
		Log(rf.me, "Grant vote to "+strconv.Itoa(args.CandidateId)+".")
		rf.votedFor = args.CandidateId
	} else {
		Log(rf.me, "Refuse to grant vote to "+strconv.Itoa(args.CandidateId)+".")
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (rf *Raft) initAppendEntriesArgs() *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = len(rf.log) - 2
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	} else {
		args.PrevLogTerm = 0
	}
	args.LeaderCommit = rf.commitIndex
	return &args
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// leader don't receive AppendEntries requests that from itself
	if rf.me == args.LeaderId {
		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}

	Log(rf.me, "Receive heartbeat from "+strconv.Itoa(args.LeaderId)+" with term = "+strconv.Itoa(args.Term))
	reply.Term = rf.currentTerm
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// deny any requests that term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = followerState
		rf.votedFor = -1
		rf.timer = electionTimeout()
	}

	if rf.state == leaderState {
		log.Fatal("Leader received an AppendEntry request with same term from other server.")
	}
	// If AppendEntries RPC received from new leader: convert to follower
	if rf.state == candidateState {
		rf.state = followerState
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if len(rf.log)-1 < args.PrevLogIndex {
		return
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[:args.PrevLogIndex]
	}
	// Append any new entries not already in the log
	for _, entry := range args.Entries {
		rf.log = append(rf.log, entry)
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		// TODO: commit logs if possible
	}

	// update server state
	rf.leaderId = args.LeaderId
	Log(rf.me, "Reset timer.")
	rf.timer = electionTimeout()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := len(rf.log)
	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (3B).

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.timer == 0 {
			Log(rf.me, "Timeout! current state: "+rf.state.stateStr())
			// ===== handle TIMEOUT =====
			if rf.state == leaderState {
				// heartbeat period
				rf.timer = heartbeatTimeout()
				go rf.sendHeartbeats()
			} else {
				// election timeout
				rf.state = candidateState
				rf.currentTerm += 1
				rf.votedFor = rf.me // vote for self
				rf.timer = electionTimeout()
				Log(rf.me, "Become candidate, start election, current term = "+strconv.Itoa(rf.currentTerm))
				go rf.startElection()
			}
			// ==========================
		}

		ms := min(40, rf.timer)
		rf.timer -= ms
		rf.mu.Unlock()

		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	// send vote requests
	args := rf.initRequestVoteArgs()
	ch := make(chan RequestVoteReply)
	for i := range rf.peers {
		go func(serverId int) {
			reply := RequestVoteReply{0, false}
			// endless retry until RPC return successfully or server state change
			for {
				currentTerm, currentState := rf.raftState()
				if currentTerm != args.Term || currentState != candidateState {
					ch <- reply
					return
				}

				ok := rf.sendRequestVote(serverId, args, &reply)
				if ok {
					break
				}
			}
			ch <- reply
		}(i)
	}

	// count votes
	voteCount := 0
	for range rf.peers {
		reply := <-ch

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			Log(rf.me, "Received vote reply with higher term. Become Follower!")
			rf.currentTerm = reply.Term
			rf.state = followerState
			rf.timer = electionTimeout()
			voteCount = -len(rf.peers) // will not become leader for these term
		}
		rf.mu.Unlock()

		if reply.Granted {
			voteCount += 1
			if voteCount == (len(rf.peers)/2)+1 {
				Log(rf.me, "Received granted votes from majority. Become Leader!")
				// win election!
				rf.mu.Lock()
				rf.state = leaderState
				rf.timer = heartbeatTimeout()
				rf.nextIndex = nil
				rf.matchIndex = nil
				for range rf.peers {
					rf.nextIndex = append(rf.nextIndex, len(rf.log))
					rf.matchIndex = append(rf.matchIndex, 0)
				}
				rf.mu.Unlock()

				go rf.sendHeartbeats()
			}
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	Log(rf.me, "Sending heartbeats to all servers.")
	args := rf.initAppendEntriesArgs()

	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	} else {
		args.PrevLogTerm = 0
	}
	args.LeaderCommit = rf.commitIndex
	for i := range rf.peers {
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, args, &reply)
	}
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
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.dead = 0
	rf.timer = electionTimeout()
	rf.leaderId = -1
	rf.state = followerState
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Log(me, "New server created!")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func Log(serverId int, message string) {
	log.Printf("server #%v : %v", serverId, message)
}
