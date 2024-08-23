package kvsrv

type RequestId struct {
	ClientId int64
	Seq      int64
}

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	RequestId RequestId
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
