package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType  string
	Filename  string
	TaskOrder int
	Nreduce   int
}

type SubmitTaskArgs struct {
	TaskType  string
	TaskOrder int
}

type SubmitTaskReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
