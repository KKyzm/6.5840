package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

// Common
type RequestTag struct {
	ClientId int
	Seq      int
}
type ReplyStatus struct {
	WrongLeader bool
	Err         Err
}

// Join
type RawJoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}
type JoinArgs struct {
	RequestTag
	RawJoinArgs
}
type JoinReply struct {
	ReplyStatus
}

// Leave
type RawLeaveArgs struct {
	GIDs []int
}
type LeaveArgs struct {
	RequestTag
	RawLeaveArgs
}
type LeaveReply struct {
	ReplyStatus
}

// Move
type RawMoveArgs struct {
	Shard int
	GID   int
}
type MoveArgs struct {
	RequestTag
	RawMoveArgs
}
type MoveReply struct {
	ReplyStatus
}

// Query
type RawQueryArgs struct {
	Num int // desired config number
}
type QueryArgs struct {
	RequestTag
	RawQueryArgs
}
type QueryReply struct {
	ReplyStatus
	Config Config
}

// Ctrler
type CtrlerArgs struct {
	Op string
	RequestTag
	RawJoinArgs
	RawLeaveArgs
	RawMoveArgs
	RawQueryArgs
}
type CtrlerReply struct {
	ReplyStatus
	Config Config
}
