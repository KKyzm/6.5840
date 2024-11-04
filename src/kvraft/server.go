package kvraft

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	Type  string
	Key   string
	Value string

	ClientId int
	Seq      int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	idMoCh  chan int
	kvstore map[string]string
	replied map[int]replyCache
	pending map[int]pendingInfo
}

type replyCache struct {
	result string
	seq    int
}

type pendingInfo struct {
	ch       chan bool
	clientId int
	seq      int
}

// Get RPC handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.log(fmt.Sprintf("Get request received, ClientId = %v, Seq = %v, Key = %v.", args.ClientId, args.Seq, args.Key))
	if args.Seq <= kv.replied[args.ClientId].seq {
		reply.Err = OK
		reply.Value = kv.replied[args.ClientId].result
		kv.log("Request has already been replied, return.")
		return
	}

	index, _, _ := kv.rf.Start(Op{"Get", args.Key, "", args.ClientId, args.Seq})
	kv.log("Launch a log, index = " + strconv.Itoa(index) + ", waiting...")
	ch := make(chan bool)
	kv.pending[index] = pendingInfo{ch, args.ClientId, args.Seq}
	kv.notifyMo()

	kv.mu.Unlock()
	ok := <-ch
	kv.mu.Lock()

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	kv.log("Launched log applied!")
	value, ok := kv.kvstore[args.Key]
	if ok {
		reply.Value = value
		reply.Err = OK
		kv.log("Get handler return successfully with value = " + reply.Value + ".")
	} else {
		reply.Err = ErrNoKey
		kv.log("Get handler return with ErrNoKey.")
	}
}

// Put/Append RPC handler
func (kv *KVServer) putAppend(op string, args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			kv.mu.Unlock()
		}
	}()

	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.log(fmt.Sprintf("%v request received, ClientId = %v, Seq = %v, Key = %v, Value = %v.", op, args.ClientId, args.Seq, args.Key, args.Value))
	if args.Seq == kv.replied[args.ClientId].seq {
		reply.Err = OK
		kv.log("Request has already been replied.")
		return
	}

	index, _, _ := kv.rf.Start(Op{op, args.Key, args.Value, args.ClientId, args.Seq})
	kv.log("Launch a log, index = " + strconv.Itoa(index) + ", waiting...")
	ch := make(chan bool)
	kv.pending[index] = pendingInfo{ch, args.ClientId, args.Seq}
	kv.notifyMo()

	unlocked = true
	kv.mu.Unlock()

	ok := <-ch
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	kv.log("Launched log applied!")
	reply.Err = OK
	kv.log(op + " handler return.")
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend("Put", args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.putAppend("Append", args, reply)
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.log("ApplyMsg received from applyCh.")
		if msg.CommandValid {
			// apply log to kvstore
			kv.mu.Lock()
			op := msg.Command.(Op)
			kv.log(fmt.Sprintf("Applied msg is command, log index = %v, clientId = %v, Seq = %v, Op = %v, Key = %v, Value = %v.", msg.CommandIndex, op.ClientId, op.Seq, op.Type, op.Key, op.Value))
			cache := replyCache{}
			cache.seq = op.Seq
			if kv.replied[op.ClientId].seq < op.Seq {
				switch op.Type {
				case "Get":
					value, ok := kv.kvstore[op.Key]
					if ok {
						cache.result = value
						kv.replied[op.ClientId] = cache
					}
				case "Put":
					kv.replied[op.ClientId] = cache
					kv.kvstore[op.Key] = op.Value
				case "Append":
					kv.replied[op.ClientId] = cache
					kv.kvstore[op.Key] += op.Value
				}
			}

			info, ok := kv.pending[msg.CommandIndex]
			if ok {
				res := info.clientId == op.ClientId && info.seq == op.Seq
				info.ch <- res
				delete(kv.pending, msg.CommandIndex)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) identityMonitor() {
	for !kv.killed() {
		<-kv.idMoCh
		for {
			_, isleader := kv.rf.GetState()
			if !isleader {
				kv.log("Lose Leadership... notify all pending handler.")
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		kv.mu.Lock()
		for _, info := range kv.pending {
			info.ch <- false
		}
		kv.pending = make(map[int]pendingInfo)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) notifyMo() {
	select {
	case kv.idMoCh <- 0:
	default:
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.idMoCh = make(chan int)
	kv.kvstore = make(map[string]string)
	kv.replied = make(map[int]replyCache)
	kv.pending = make(map[int]pendingInfo)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// launch background goroutine to keep reading applyCh and apply it on kvstore
	go kv.identityMonitor()
	go kv.applier()

	return kv
}

func (kv *KVServer) log(message string) {
	log.Printf("Server #%v : %v", kv.me, message)
}
