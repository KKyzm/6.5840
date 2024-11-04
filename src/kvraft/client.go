package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"strconv"
	"time"

	"6.5840/labrpc"
)

// NOTE: It's OK to assume that a client will make only one call into a Clerk at a time
type Clerk struct {
	servers    []*labrpc.ClientEnd
	clientId   int
	leaderId   int
	requestSeq int // request's seq, starts from 1
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = int(nrand())
	ck.leaderId = -1
	ck.requestSeq = 1
	return ck
}

func (ck *Clerk) initGetArgs(key string) *GetArgs {
	args := &GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.Seq = ck.requestSeq
	ck.requestSeq += 1
	return args
}

func (ck *Clerk) initPutAppArgs(key string, value string) *PutAppendArgs {
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.ClientId = ck.clientId
	args.Seq = ck.requestSeq
	ck.requestSeq += 1
	return args
}

func (ck *Clerk) serverEnumerator(ch chan<- int, done chan int) {
	if ck.leaderId != -1 {
		ch <- ck.leaderId
	}

	cnt := 0
	for {
		if cnt > 0 {
			time.Sleep(500 * time.Millisecond)
		}
		for i := range ck.servers {
			select {
			case ch <- i:
			case <-done:
				return
			}
		}
		cnt++
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.log("Make a Get request, Key = " + key + ".")

	ch := make(chan int)
	done := make(chan int)
	go ck.serverEnumerator(ch, done)
	defer close(done)

	args := ck.initGetArgs(key)
	for i := range ch { // keep trying forever
		ck.log("Requesting to server #" + strconv.Itoa(i) + ".")
		reply := &GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", args, reply)
		if !ok {
			continue
		}
		if reply.Err != ErrWrongLeader {
			ck.leaderId = i
			if reply.Err == OK {
				ck.log("Get value = " + reply.Value + ", return.")
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				ck.log("KVstore has no key " + key + ", return.")
				return ""
			}
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.log(fmt.Sprintf("Make a %v request, Key = %v, Value = %v", op, key, value))

	ch := make(chan int)
	done := make(chan int)
	go ck.serverEnumerator(ch, done)
	defer close(done)

	args := ck.initPutAppArgs(key, value)
	for i := range ch { // keep trying forever
		ck.log("Requesting to server #" + strconv.Itoa(i) + ".")
		reply := &PutAppendReply{}
		ok := ck.servers[i].Call("KVServer."+op, args, reply)
		if !ok {
			continue
		}
		if reply.Err == OK {
			ck.leaderId = i
			ck.log(op + " return.")
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) log(message string) {
	log.Printf("Client #%v : %v", ck.clientId, message)
}
