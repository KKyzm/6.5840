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

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	followerState  serverState = 1 << iota
	candidateState             = 2
	leaderState                = 3
)

func (state *serverState) str() string {
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
	heartbeatPeriod               int64 = 200
	electionTimeoutBase           int64 = 800
	electionTimeoutVariationRange int64 = 400
	delayTolerance                      = 10
)

func heartbeatTimeout() int64 {
	return heartbeatPeriod
}

func electionTimeout() int64 {
	return electionTimeoutBase + (rand.Int63() % electionTimeoutVariationRange)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	leaderElectedCh []chan int
	requestGotCh    []chan int
	persister       *Persister // Object to hold this peer's persisted state
	me              int        // this peer's index into peers[]
	dead            int32      // set by Kill()

	// Your data here (3A, 3B, 3C).
	timer         int64
	leaderId      int
	applyCh       chan ApplyMsg
	applyNotifyCh chan int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leaderState
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	labgob.Register(LogEntry{})

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("Failed to decode raft persistence...")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term    int
	Granted bool
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	Log(rf.me, "Receive vote request from "+strconv.Itoa(args.CandidateId)+".")
	// res := rf.checkReceivedTerm(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Granted = false

	newTermAtFollower := false
	if rf.currentTerm > args.Term {
		// deny any requests that term < currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != followerState {
			Log(rf.me, "Received vote request with higher term. Become Follower!")
			rf.state = followerState
			rf.timer = electionTimeout()
		} else {
			newTermAtFollower = true
		}
		rf.persist()
	}

	// If votedFor is null or candidateId, and candidate’s log is
	// at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex := len(rf.log) - 1
		if lastLogIndex < 0 {
			reply.Granted = true
		} else if rf.log[lastLogIndex].Term < args.LastLogTerm {
			reply.Granted = true
		} else if rf.log[lastLogIndex].Term == args.LastLogTerm {
			reply.Granted = (lastLogIndex <= args.LastLogIndex)
		}
	}

	if reply.Granted {
		Log(rf.me, "Grant vote to "+strconv.Itoa(args.CandidateId)+".")
		rf.votedFor = args.CandidateId
		if newTermAtFollower {
			rf.timer = electionTimeout()
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) {
	startTime := time.Now()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	elapsed := time.Since(startTime)
	if elapsed.Milliseconds() < heartbeatTimeout()-delayTolerance {
		ch <- ok
	}
}

type AppendEntriesArgs struct {
	Entries      []LogEntry
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) initAppendEntriesArgs(i int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leaderState {
		return nil
	}

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	} else {
		args.PrevLogTerm = 0
	}
	if rf.nextIndex[i] >= 0 && rf.nextIndex[i] < len(rf.log) {
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]])
	}
	args.LeaderCommit = rf.commitIndex
	Log(rf.me, fmt.Sprintf("Form AppendEntriesArgs for peer-%v with Term = %v, PrevLogIndex = %v, PrevLogTerm = %v, leaderCommit = %v", i, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit))
	return &args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	assert(rf.me != args.LeaderId, "Leader won't send AppendEntry to itself.")
	assert(args.PrevLogIndex >= -1, "PrevLogIndex field in AppendEntriesArgs is at least -1.")

	Log(rf.me, "Receive AppendEntry request from "+strconv.Itoa(args.LeaderId)+" with term = "+strconv.Itoa(args.Term)+", prev log index = "+strconv.Itoa(args.PrevLogIndex)+", num of entries = "+strconv.Itoa(len(args.Entries))+".")
	reply.Term = rf.currentTerm
	reply.Success = false

	res := rf.checkReceivedTerm(args.Term)
	if res < 0 {
		// deny any requests that term < currentTerm
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	assert(rf.state != leaderState, "Leader will never receive an AppendEntry request with SAME term from other server.")
	// If AppendEntries RPC received from new leader: convert to follower
	if rf.state == candidateState {
		rf.state = followerState
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if len(rf.log)-1 < args.PrevLogIndex {
		Log(rf.me, "Current log doesn’t contain an entry at prevLogIndex.")
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log) - 1
		Log(rf.me, fmt.Sprintf("Set ConflictTerm = %v, ConflictIndex = %v.", reply.ConflictTerm, reply.ConflictIndex))
		return
	}
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Log(rf.me, "Current log entry' term in prevLogIndex doesn't matches prevLogTerm.")
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		index := args.PrevLogIndex
		for index > 0 {
			if rf.log[index-1].Term != reply.ConflictTerm {
				break
			}
			index--
		}
		reply.ConflictIndex = index
		Log(rf.me, fmt.Sprintf("Set ConflictTerm = %v, ConflictIndex = %v.", reply.ConflictTerm, reply.ConflictIndex))
		return
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	rf.log = rf.log[:args.PrevLogIndex+1]
	// Append any new entries not already in the log
	for _, entry := range args.Entries {
		rf.log = append(rf.log, entry)
	}
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		go func() {
			rf.applyNotifyCh <- 0
		}()
	}

	// update server state
	rf.leaderId = args.LeaderId
	rf.votedFor = args.LeaderId
	rf.persist()
	rf.timer = electionTimeout()
	reply.Success = true
	Log(rf.me, "AppendEntry request return succseefully.")
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, ch chan bool) {
	startTime := time.Now()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	elapsed := time.Since(startTime)
	if elapsed.Milliseconds() < heartbeatTimeout()-delayTolerance {
		ch <- ok
	}
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
	// Your code here (3B).
	// state check
	if rf.killed() {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log) + 1 // from client's view, log index starts from 1, not 0
	// state check
	if rf.state != leaderState {
		return index, rf.currentTerm, false
	}

	// insert entry into leader's log record
	entry := LogEntry{command, rf.currentTerm}
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.matchIndex[rf.me] = len(rf.log) - 1
	Log(rf.me, "New log entry arrived, current log overview: "+rf.logOverview())
	// notify leader duty goroutine to handle log replication task
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, lastLogIdex int) {
			rf.requestGotCh[i] <- lastLogIdex
		}(i, len(rf.log)-1)
	}

	return index, rf.currentTerm, true
}

// for debug
func (rf *Raft) logRangeOverview(start int, end int) string {
	res := "["
	for i := start; i < end; i++ {
		entry := rf.log[i]
		res += strconv.Itoa(entry.Term) + " "
	}
	res = strings.TrimSpace(res)
	res += "]"
	return res
}

// for debug
func (rf *Raft) logOverview() string {
	return rf.logRangeOverview(0, len(rf.log))
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
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Tick until election timeout.
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.timer == 0 {
			Log(rf.me, "Timeout! current state: "+rf.state.str())
			if rf.state != leaderState {
				rf.state = candidateState
				rf.currentTerm += 1
				rf.votedFor = rf.me // vote for self
				rf.timer = electionTimeout()
				rf.persist()
				Log(rf.me, "Become candidate, start election, current term = "+strconv.Itoa(rf.currentTerm))
				go rf.startElection(rf.currentTerm)
			} else {
				rf.timer = 20
			}
		}

		var ms int64
		if rf.timer < 0 {
			ms = 20
		} else if rf.timer == 0 {
			panic("Timer should never equals to 0 there.")
		} else {
			ms = min(20, rf.timer)
			rf.timer -= ms
		}
		// Log(rf.me, "Tick for "+strconv.Itoa(int(ms))+"ms, rf.timer remains "+strconv.Itoa(int(rf.timer))+"ms.")
		rf.mu.Unlock()

		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Initiate a round of leader election.
func (rf *Raft) startElection(electionTerm int) {
	voteReplyCh := make(chan RequestVoteReply)
	// send vote requests
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			args := rf.initRequestVoteArgs()
			// endless retry until RPC return successfully or server state change
			for {
				reply := &RequestVoteReply{0, false}

				rf.mu.Lock()
				currentTerm := rf.currentTerm
				state := rf.state
				rf.mu.Unlock()

				Log(rf.me, fmt.Sprintf("Current state: term = %v, state = %v.", currentTerm, state.str()))
				if currentTerm != args.Term || state != candidateState {
					Log(rf.me, "State changed, stop sending vote request to server "+strconv.Itoa(serverId)+".")
					voteReplyCh <- *reply
					return
				}

				Log(rf.me, "Sending vote request to server "+strconv.Itoa(serverId)+".")

				ch := make(chan bool)
				go rf.sendRequestVote(serverId, args, reply, ch)

				select {
				case <-time.After(time.Duration(heartbeatTimeout()) * time.Millisecond):
					Log(rf.me, "Vote request to server "+strconv.Itoa(serverId)+" TIMEOUT, retry.")
				case ok := <-ch:
					if ok {
						if reply.Granted {
							Log(rf.me, "Received vote reply from server "+strconv.Itoa(serverId)+", GRANTED!")
						} else {
							Log(rf.me, "Received vote reply from server "+strconv.Itoa(serverId)+", UNGRANTED...")
						}
						voteReplyCh <- *reply
						return
					}
					Log(rf.me, "Vote request to server "+strconv.Itoa(serverId)+" FAILED, retry.")
					time.Sleep(time.Duration(heartbeatTimeout()) * time.Millisecond)
				}
			}
		}(i)
	}

	// count votes
	voteCount := 1
	for range rf.peers {
		reply := <-voteReplyCh

		rf.checkReceivedTerm(reply.Term)

		rf.mu.Lock()
		if rf.currentTerm != electionTerm || rf.state != candidateState {
			rf.mu.Unlock()
			continue
		}

		if reply.Granted {
			voteCount += 1
			if voteCount == (len(rf.peers)/2)+1 {
				Log(rf.me, "Received granted votes from majority. Become Leader!")
				// win election!
				rf.state = leaderState
				rf.timer = -1
				rf.nextIndex = nil
				rf.matchIndex = nil
				for range rf.peers {
					rf.nextIndex = append(rf.nextIndex, len(rf.log))
					rf.matchIndex = append(rf.matchIndex, -1)
				}
				rf.matchIndex[rf.me] = len(rf.log) - 1
				lastLogIndex := len(rf.log) - 1
				// notify leader duty goroutines to work
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					go func(i int, lastLogIndex int) {
						rf.leaderElectedCh[i] <- lastLogIndex
					}(i, lastLogIndex)
				}
			}
		}
		rf.mu.Unlock()
	}
}

// If received term is larger than current term, turn server state
// to follower, update corresponding parameter and return 1,
// otherwise return 0 for equal and -1 for smaller.
func (rf *Raft) checkReceivedTerm(term int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		Log(rf.me, "Received request/reply with higher term. Become Follower!")
		rf.currentTerm = term
		rf.votedFor = -1
		rf.state = followerState
		rf.timer = electionTimeout()
		rf.persist()
		return 1
	}
	if term == rf.currentTerm {
		return 0
	}
	return -1
}

// Goroutine that fulfilling leader's duty to each server (heartbeat and log replication).
func (rf *Raft) leaderDuty(i int) {
	for !rf.killed() {
		lastLogIndex := <-rf.leaderElectedCh[i]

		Log(rf.me, "Begin to fulfill leader duty to peer-"+strconv.Itoa(i)+".")
		for !rf.killed() {
			args := rf.initAppendEntriesArgs(i)
			// check state
			if args == nil {
				Log(rf.me, "AppendEntriesArgs is nil, stop leader duty.")
				break
			}
			// non-block channel operation to check if new log record is arrived
			select {
			case lastLogIndex = <-rf.requestGotCh[i]:
				Log(rf.me, "New log record needed to send to peer-"+strconv.Itoa(i)+".")
			default:
				// No new record
			}
			ch := make(chan bool)
			go rf.sendAppendEntryWithTimeout(i, args, lastLogIndex, ch)

			select {
			case <-time.After(time.Duration(heartbeatTimeout()) * time.Millisecond):
				Log(rf.me, "Heartbeat timeout on peer-"+strconv.Itoa(i)+".")
			case success := <-ch:
				Log(rf.me, "AppendEntry reply received from peer-"+strconv.Itoa(i)+" with success = "+strconv.FormatBool(success)+", replicate more logs.")
			}
		}
		Log(rf.me, "Stop to fulfill leader duty to peer-"+strconv.Itoa(i)+".")
	}
}

func (rf *Raft) sendAppendEntryWithTimeout(i int, args *AppendEntriesArgs, lastLogIndex int, ch chan bool) {
	Log(rf.me, "Sending AppendEntry request to peer-"+strconv.Itoa(i)+".")
	reply := &AppendEntriesReply{}
	requestFinishedCh := make(chan bool)

	go rf.sendAppendEntries(i, args, reply, requestFinishedCh)
	var ok bool
	select {
	case <-time.After(time.Duration(heartbeatTimeout()) * time.Millisecond):
		Log(rf.me, "AppendEntry request to peer-"+strconv.Itoa(i)+" TIMEOUT.")
		return
	case ok = <-requestFinishedCh:
	}
	if !ok {
		return
	}

	res := rf.checkReceivedTerm(reply.Term)
	if res > 0 {
		return
	}
	if !reply.Success {
		rf.mu.Lock()
		if reply.ConflictTerm < 0 {
			rf.nextIndex[i] = reply.ConflictIndex + 1
		} else {
			assert(reply.ConflictIndex >= 0, "Conflict index should not less than 0.")
			idx := args.PrevLogIndex - 1
			for idx >= reply.ConflictIndex {
				if rf.log[idx].Term == reply.ConflictTerm {
					break
				}
				idx--
			}
			rf.nextIndex[i] = idx + 1
		}
		Log(rf.me, "Set rf.nextIndex for "+strconv.Itoa(i)+" = "+strconv.Itoa(rf.nextIndex[i])+".")
		rf.mu.Unlock()
		ch <- false
		return
	}
	rf.logReplicated(i, args.PrevLogIndex+len(args.Entries))
	rf.mu.Lock()
	rf.nextIndex[i] += len(args.Entries)
	nextIndexFori := rf.nextIndex[i]
	rf.mu.Unlock()
	if nextIndexFori <= lastLogIndex {
		ch <- true
	}
}

// Inform leader that the server has replicated the log,
// leader should update rf.matchIndex and commit log if possible.
func (rf *Raft) logReplicated(serverId int, logIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.matchIndex[serverId] >= logIndex {
		return
	}

	Log(rf.me, "Log that index = "+strconv.Itoa(logIndex)+" has been replicated on peer-"+strconv.Itoa(serverId)+".")
	rf.matchIndex[serverId] = logIndex

	var matches []int
	matches = append(matches, rf.matchIndex...)
	sort.Ints(matches)
	majorityReplicatedIndex := matches[len(matches)/2]
	if majorityReplicatedIndex < 0 || rf.log[majorityReplicatedIndex].Term < rf.currentTerm {
		return
	}
	committedLogIndex := majorityReplicatedIndex
	if rf.commitIndex >= committedLogIndex {
		return
	}
	rf.commitIndex = committedLogIndex
	Log(rf.me, "Update commitIndex = "+strconv.Itoa(rf.commitIndex)+".")

	go func() {
		// Log(rf.me, "=== Begin === notify to apply committed log.")
		rf.applyNotifyCh <- 0
		// Log(rf.me, "==== End ==== notify to apply committed log.")
	}()
}

func (rf *Raft) applyCommittedLog() {
	for !rf.killed() {
		<-rf.applyNotifyCh
		// Log(rf.me, "Apply notification receive.")
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for i := lastApplied + 1; i <= commitIndex; i++ {
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = rf.log[i].Command
			applyMsg.CommandIndex = i + 1
			// Log(rf.me, "=== Begin === apply committed log of index "+strconv.Itoa(i)+".")
			rf.applyCh <- applyMsg
			// Log(rf.me, "==== End ==== apply committed log of index "+strconv.Itoa(i)+".")
		}

		rf.mu.Lock()
		Log(rf.me, fmt.Sprintf("Applying committed log from index %v to %v, applied log: %v", lastApplied+1, commitIndex, rf.logRangeOverview(0, commitIndex+1)))
		rf.mu.Unlock()
	}
}

func assert(state bool, msg string) {
	if !state {
		pc, file, no, ok := runtime.Caller(1)
		details := runtime.FuncForPC(pc)
		if ok {
			log.Fatal("Assert failed [" + filepath.Base(file) + ":" + strconv.Itoa(no) + " " + details.Name() + "]: " + msg)
		} else {
			log.Fatal("Assert failed: " + msg)
		}
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
	for range rf.peers {
		rf.leaderElectedCh = append(rf.leaderElectedCh, make(chan int))
		rf.requestGotCh = append(rf.requestGotCh, make(chan int))
	}
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.dead = 0
	rf.timer = electionTimeout()
	rf.leaderId = -1
	rf.applyCh = applyCh
	rf.applyNotifyCh = make(chan int)
	rf.state = followerState
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	Log(me, "Peer resume!")

	go rf.applyCommittedLog() // background goroutine to apply committed log when notified
	go rf.electionTicker()    // ticker goroutine to start elections
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.leaderDuty(i)
	}

	return rf
}

func Log(serverId int, message string) {
	log.Printf("Peer #%v : %v", serverId, message)
}
