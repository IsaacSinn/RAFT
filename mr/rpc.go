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

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType        string
	Filename        string
	NReduce         int
	JobID           int
	TotalMapJobs    int
	AssignmentCount int
}

type ReportTaskDoneArgs struct {
	FileName        string
	TaskID          int
	TaskType        string
	TempFilesNames  []string // not used for reduce
	AssignmentCount int      // not used for reduce
}

type ReportTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
