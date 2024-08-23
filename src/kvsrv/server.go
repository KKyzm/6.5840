package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	kvstore map[string]string
	replied map[int64]*replyCache
	mu      sync.Mutex
}

type replyCache struct {
	result string
	offset int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvstore[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	res, ok := kv.isReplied(args.RequestId)
	if ok {
		reply.Value = res
		return
	}

	kv.kvstore[args.Key] = args.Value
	kv.cacheRequest(args.RequestId, "")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	res, ok := kv.isReplied(args.RequestId)
	if ok {
		reply.Value = res
		return
	}

	reply.Value = kv.kvstore[args.Key]
	kv.kvstore[args.Key] += args.Value
	kv.cacheRequest(args.RequestId, reply.Value)
}

func (kv *KVServer) isReplied(request_id RequestId) (string, bool) {
	cache, ok := kv.replied[request_id.ClientId]
	if ok {
		if cache.offset > request_id.Seq {
			return "", true
		}
		if cache.offset == request_id.Seq {
			return cache.result, true
		}
	}
	return "", false
}

func (kv *KVServer) cacheRequest(request_id RequestId, res string) {
	client_id := request_id.ClientId
	kv.replied[client_id] = &replyCache{res, request_id.Seq}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.kvstore = map[string]string{}
	kv.replied = map[int64]*replyCache{}

	return kv
}
