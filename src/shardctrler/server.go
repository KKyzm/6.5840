package shardctrler

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	idMoCh chan int
	// formSsCh chan int
	configs []Config            // indexed by config num
	replied map[int]replyCache  // client id -> reply cache
	pending map[int]pendingInfo // log index -> info of pending request
}

type replyCache struct {
	Conf Config
	Seq  int
}

type pendingInfo struct {
	ch       chan bool
	clientId int
	seq      int
}

func (sc *ShardCtrler) joinLeaveMoveQuery(args *CtrlerArgs, reply *CtrlerReply) {
	var unlocked bool
	lock := func() { unlocked = false; sc.mu.Lock() }
	unlock := func() { unlocked = true; sc.mu.Unlock() }

	lock()
	defer func() {
		if !unlocked {
			sc.mu.Unlock()
		}
	}()

	// ---

	op := args.Op
	sc.log(fmt.Sprintf("Request received, Type = %v, ClientId = %v, Seq = %v.", op, args.ClientId, args.Seq))
	assert(args.Seq >= sc.replied[args.ClientId].Seq, "Seq of request from a client should always bigger or equal to last seq in cache.")
	if args.Seq == sc.replied[args.ClientId].Seq {
		reply.WrongLeader = false
		reply.Err = OK
		if op == "Query" {
			reply.Config = sc.replied[args.ClientId].Conf
		}
		sc.log(fmt.Sprintf("%v request has already been replied, return.", op))
		return
	}

	index, _, _ := sc.rf.Start(*args)
	sc.log("Launch a log, index = " + strconv.Itoa(index) + ", waiting...")
	ch := make(chan bool)
	sc.pending[index] = pendingInfo{ch, args.ClientId, args.Seq}
	sc.notifyMo()

	unlock()

	ok := <-ch
	if ok {
		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
		return
	}

	lock()
	if op == "Query" {
		reply.Config = sc.replied[args.ClientId].Conf
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return
	}

	cargs := &CtrlerArgs{}
	cargs.Op = "Join"
	cargs.ClientId = args.ClientId
	cargs.Seq = args.Seq
	cargs.Servers = args.Servers
	creply := &CtrlerReply{}
	sc.joinLeaveMoveQuery(cargs, creply)
	reply.Err = creply.Err
	reply.WrongLeader = creply.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return
	}

	cargs := &CtrlerArgs{}
	cargs.Op = "Leave"
	cargs.ClientId = args.ClientId
	cargs.Seq = args.Seq
	cargs.GIDs = args.GIDs
	creply := &CtrlerReply{}
	sc.joinLeaveMoveQuery(cargs, creply)
	reply.Err = creply.Err
	reply.WrongLeader = creply.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return
	}

	cargs := &CtrlerArgs{}
	cargs.Op = "Move"
	cargs.ClientId = args.ClientId
	cargs.Seq = args.Seq
	cargs.Shard = args.Shard
	cargs.GID = args.GID
	creply := &CtrlerReply{}
	sc.joinLeaveMoveQuery(cargs, creply)
	reply.Err = creply.Err
	reply.WrongLeader = creply.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	_, isleader := sc.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return
	}

	cargs := &CtrlerArgs{}
	cargs.Op = "Query"
	cargs.ClientId = args.ClientId
	cargs.Seq = args.Seq
	cargs.Num = args.Num
	creply := &CtrlerReply{}
	sc.joinLeaveMoveQuery(cargs, creply)
	reply.Err = creply.Err
	reply.WrongLeader = creply.WrongLeader
	reply.Config = creply.Config
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.log("ApplyMsg received from applyCh.")
		if msg.CommandValid {
			// apply log to configs
			sc.mu.Lock()

			cmd := msg.Command.(CtrlerArgs)
			clientId := cmd.ClientId
			seq := cmd.Seq

			sc.log(fmt.Sprintf("Applied msg is command, log index = %v, clientId = %v, seq = %v.", msg.CommandIndex, clientId, seq))
			cache := replyCache{}
			cache.Seq = seq
			if sc.replied[clientId].Seq < seq {
				switch cmd.Op {
				case "Join":
					keys := make([]int, 0, len(cmd.Servers))
					for key := range cmd.Servers {
						keys = append(keys, key)
					}
					sc.log("Applying Join command, new groups: " + strings.Trim(strings.ReplaceAll(fmt.Sprint(keys), " ", " "), "[]"))
					sc.applyJoin(cmd.Servers)
				case "Leave":
					sc.log("Applying Leave command, leaving groups: " + strings.Trim(strings.ReplaceAll(fmt.Sprint(cmd.GIDs), " ", " "), "[]"))
					sc.applyLeave(cmd.GIDs)
				case "Move":
					sc.log(fmt.Sprintf("Applying Move command, shard #%v move to group #%v", cmd.Shard, cmd.GID))
					sc.applyMove(cmd.Shard, cmd.GID)
				case "Query":
					sc.log(fmt.Sprintf("Applying Query command, querying config num: #%v", cmd.Num))
					conf := sc.applyQuery(cmd.Num)
					cache.Conf = conf
				default:
					log.Fatal("Unknown cmd type :" + cmd.Op + "!")
				}
				sc.replied[clientId] = cache
			}

			info, ok := sc.pending[msg.CommandIndex]
			if ok {
				sc.log("Notify pending RPC handler.")
				res := info.clientId == clientId && info.seq == seq
				info.ch <- res
				delete(sc.pending, msg.CommandIndex)
			}

			sc.mu.Unlock()

		} else if msg.SnapshotValid {
			// never
		}
	}
}

func (sc *ShardCtrler) applyJoin(servers map[int][]string) {
	newConf := copyConf(sc.configs[len(sc.configs)-1]) // copy
	sc.log("Before Join, config status: " + conf2str(newConf))
	newConf.Num += 1
	for gid, servernames := range servers {
		newConf.Groups[gid] = servernames
	}

	// gids in the new configuration
	gids := make([]int, 0)
	for gid := range newConf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// how many shards each group served in OLD configuration
	oldAssignment := make(map[int]int) // gid -> num of shards
	for _, assignedGid := range newConf.Shards {
		oldAssignment[assignedGid] += 1
	}

	quotient := len(newConf.Shards) / len(newConf.Groups)
	remainder := len(newConf.Shards) % len(newConf.Groups)
	diffs := make(map[int]int)
	for i, gid := range gids {
		numOfShards := quotient // the num of shards that i_th group should serve in NEW configuration
		if i < remainder {
			numOfShards += 1
		}
		// the num of shards group(@gid) should move out(negative) or move in(positive) to achieve balance
		diffs[gid] = numOfShards - oldAssignment[gid]
	}

	positiveDiffGids := make([]int, 0)
	for gid, diff := range diffs {
		for i := 0; i < diff; i++ {
			positiveDiffGids = append(positiveDiffGids, gid)
		}
	}
	sort.Ints(positiveDiffGids)
	posDiffGidPtr := 0

	for shardId, oweGid := range newConf.Shards {
		if oweGid == 0 {
			owedGid := positiveDiffGids[posDiffGidPtr]
			newConf.Shards[shardId] = owedGid
			posDiffGidPtr += 1
			diffs[owedGid] -= 1
			// assign shard(@shardId) to new group(@owedGid)
		} else if diffs[oweGid] < 0 {
			owedGid := positiveDiffGids[posDiffGidPtr]
			newConf.Shards[shardId] = owedGid
			posDiffGidPtr += 1
			diffs[oweGid] += 1
			diffs[owedGid] -= 1
			// move shard(@shardId) from old group(@oweGid) to new group(@owedGid)
		}
	}

	sc.log("After Join, config status: " + conf2str(newConf))

	// check
	assert(posDiffGidPtr == len(positiveDiffGids), "")
	for _, d := range diffs {
		assert(d == 0, "")
	}

	// add new configuration to configs
	sc.configs = append(sc.configs, newConf)
}

func (sc *ShardCtrler) applyLeave(GIDs []int) {
	newConf := copyConf(sc.configs[len(sc.configs)-1]) // copy
	sc.log("Before Leave, config status: " + conf2str(newConf))
	newConf.Num += 1
	for _, gid := range GIDs {
		delete(newConf.Groups, gid)
	}
	// edge case check
	if len(newConf.Groups) == 0 {
		for i := 0; i < len(newConf.Shards); i++ {
			newConf.Shards[i] = 0
		}
		// add new configuration to configs
		sc.configs = append(sc.configs, newConf)
		return
	}

	// ---

	// gids should appear in the new configuration
	gids := make([]int, 0)
	for gid := range newConf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// how many shards each group served in OLD configuration
	oldAssignment := make(map[int]int) // gid -> num of shards
	for _, assignedGid := range newConf.Shards {
		oldAssignment[assignedGid] += 1
	}

	quotient := len(newConf.Shards) / len(newConf.Groups)
	remainder := len(newConf.Shards) % len(newConf.Groups)
	diffs := make(map[int]int)
	for i, gid := range gids {
		numOfShards := quotient // the num of shards that i_th group should serve in NEW configuration
		if i < remainder {
			numOfShards += 1
		}
		// the num of shards group(@gid) should move out(negative) or move in(positive) to achieve balance
		diffs[gid] = numOfShards - oldAssignment[gid]
	}
	for _, gid := range GIDs {
		diffs[gid] = -oldAssignment[gid]
	}

	positiveDiffGids := make([]int, 0)
	for gid, diff := range diffs {
		for i := 0; i < diff; i++ {
			positiveDiffGids = append(positiveDiffGids, gid)
		}
	}
	sort.Ints(positiveDiffGids)
	posDiffGidPtr := 0

	for shardId, oweGid := range newConf.Shards {
		if diffs[oweGid] < 0 {
			owedGid := positiveDiffGids[posDiffGidPtr]
			posDiffGidPtr += 1
			newConf.Shards[shardId] = owedGid
			diffs[oweGid] += 1
			diffs[owedGid] -= 1
			// move shard(@shardId) from old group(@oweGid) to new group(@owedGid)
		}
	}
	sc.log("After Leave, config status: " + conf2str(newConf))

	// check
	assert(posDiffGidPtr == len(positiveDiffGids), "")
	for _, d := range diffs {
		assert(d == 0, "")
	}

	// add new configuration to configs
	sc.configs = append(sc.configs, newConf)
}

func (sc *ShardCtrler) applyMove(Shard int, GID int) {
	newConf := copyConf(sc.configs[len(sc.configs)-1]) // copy
	sc.log("Before Move, config status: " + conf2str(newConf))
	newConf.Num += 1
	newConf.Shards[Shard] = GID
	sc.log("After Move, config status: " + conf2str(newConf))
	sc.configs = append(sc.configs, newConf)
}

func (sc *ShardCtrler) applyQuery(Num int) Config {
	var conf Config
	if Num < 0 || Num >= len(sc.configs) {
		conf = sc.configs[len(sc.configs)-1]
	} else {
		conf = sc.configs[Num]
	}
	sc.log("Query result, config status: " + conf2str(conf))
	return copyConf(conf)
}

func (sc *ShardCtrler) identityMonitor() {
	for !sc.killed() {
		<-sc.idMoCh
		for {
			_, isleader := sc.rf.GetState()
			if !isleader {
				sc.log("Lose Leadership... notify all pending handler.")
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		sc.mu.Lock()
		for _, info := range sc.pending {
			info.ch <- false
		}
		sc.pending = make(map[int]pendingInfo)
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) notifyMo() {
	select {
	case sc.idMoCh <- 0:
	default:
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	// sc.maxraftstate = maxraftstate
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.idMoCh = make(chan int)
	// sc.formSsCh = make(chan int)
	// sc.lastAppliedIndex = 0
	sc.persister = persister
	sc.replied = make(map[int]replyCache)
	sc.pending = make(map[int]pendingInfo)
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(CtrlerArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	go sc.applier()
	go sc.identityMonitor()

	return sc
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

func copyConf(conf Config) Config {
	var copied Config
	copied.Num = conf.Num
	copied.Shards = conf.Shards
	copied.Groups = make(map[int][]string)
	for gid, servernames := range conf.Groups {
		copied.Groups[gid] = make([]string, 0)
		for _, servername := range servernames {
			copied.Groups[gid] = append(copied.Groups[gid], servername)
		}
	}
	return copied
}

func conf2str(conf Config) string {
	s := "\n"
	s += "\tConfig Num = " + strconv.Itoa(conf.Num) + "\n"
	s += "\tGroups = "
	gids := make([]int, 0)
	for gid := range conf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	s += strings.ReplaceAll(fmt.Sprint(gids), " ", ", ") + "\n"
	s += "\tShards assignment = " + strings.ReplaceAll(fmt.Sprint(conf.Shards), " ", ", ")
	return s
}

func (sc *ShardCtrler) log(message string) {
	log.Printf("ShardCtrler #%v : %v", sc.me, message)
}
