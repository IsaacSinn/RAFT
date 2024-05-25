package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu      sync.Mutex
	NReduce int
	// map job variables
	mapJobs            map[string]int
	mapJobAssignments  map[string]int
	totalMapJobs       int
	mapTasksRemaining  []string
	mapTasksInProgress map[string]bool
	mapTasksCompleted  map[string]bool
	mapAllDone         bool
	// reduce job variables
	reduceTasksRemaining  []int
	reduceTasksInProgress map[int]bool
	reduceTasksCompleted  map[int]bool
	allDone               bool
}

// Your code here -- RPC handlers for the worker to call.

// RPC handler for worker to call to get a task
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	c.mu.Lock()
	// check if map tasks reamining
	if len(c.mapTasksRemaining) > 0 {
		// send map task
		reply.NReduce = c.NReduce
		reply.TaskType = "map"
		reply.Filename = c.mapTasksRemaining[0]
		reply.JobID = c.mapJobs[reply.Filename]

		// remove map task from remaining and add to in progress
		c.mapJobAssignments[reply.Filename] = 1

		reply.AssignmentCount = c.mapJobAssignments[reply.Filename]
		c.mapTasksInProgress[reply.Filename] = true
		c.mapTasksRemaining = c.mapTasksRemaining[1:]

		// no task remaining but some in progress assign the ones in progress
	} else if len(c.mapTasksInProgress) > 0 {
		for k, _ := range c.mapTasksInProgress {
			reply.NReduce = c.NReduce
			reply.TaskType = "map"
			reply.Filename = k
			reply.JobID = c.mapJobs[reply.Filename]

			c.mapJobAssignments[reply.Filename] += 1
			reply.AssignmentCount = c.mapJobAssignments[reply.Filename]
			break
		}

	} else if c.mapAllDone {
		reply.TaskType = "reduce"
		reply.TotalMapJobs = c.totalMapJobs

		// check if reduce tasks remaining
		if len(c.reduceTasksRemaining) > 0 {

			reply.JobID = c.reduceTasksRemaining[0]

			// remove reduce task from remaining and add to in progress
			c.reduceTasksInProgress[reply.JobID] = true
			c.reduceTasksRemaining = c.reduceTasksRemaining[1:]
		} else if len(c.reduceTasksInProgress) > 0 {
			for k, _ := range c.reduceTasksInProgress {
				reply.JobID = k
				break
			}
		}

	}

	c.mu.Unlock()
	return nil
}

// RPC handler for worker to call to report task completion
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	if args.TaskType == "map" {

		if !c.mapTasksCompleted[args.FileName] {
			// fmt.Printf("Map task %s done, assignment count: %d\n", args.FileName, args.AssignmentCount)
			for i := 0; i < c.NReduce; i++ {
				tempFileName := fmt.Sprintf("mr-%d-%d", args.TaskID, i)
				os.Rename(args.TempFilesNames[i+10], tempFileName)
				// fmt.Printf("Renamed %s to %s\n", args.TempFilesNames[i+10], tempFileName)
			}

			// remove map task from in progress and add to completed
			delete(c.mapTasksInProgress, args.FileName)
			c.mapTasksCompleted[args.FileName] = true
			// fmt.Printf("Task %s done\n", args.FileName)
		}

		if len(c.mapTasksInProgress) == 0 && len(c.mapTasksRemaining) == 0 {
			// all map tasks are done
			c.mapAllDone = true
			// fmt.Printf("All map tasks done\n")
		}
	} else if args.TaskType == "reduce" {
		if !c.reduceTasksCompleted[args.TaskID] {
			//rename to mr-out-X
			os.Rename(args.FileName, fmt.Sprintf("mr-out-%d", args.TaskID))

			// remove reduce task from in progress and add to completed
			delete(c.reduceTasksInProgress, args.TaskID)
			c.reduceTasksCompleted[args.TaskID] = true
			// fmt.Printf("Task %d done\n", args.TaskID)
		}

		if len(c.reduceTasksInProgress) == 0 && len(c.reduceTasksRemaining) == 0 {
			// all reduce tasks are done
			c.allDone = true
			// fmt.Printf("All reduce tasks done\n")
		}

	}
	c.mu.Unlock()

	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// check if all tasks are done
	c.mu.Lock()
	ret = c.allDone
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	totalMapJobs := len(files)
	c := Coordinator{
		totalMapJobs: totalMapJobs,
		allDone:      false,
		mapAllDone:   false,
		NReduce:      nReduce,
		mu:           sync.Mutex{},
	}
	c.mapJobs = make(map[string]int)
	c.mapTasksInProgress = make(map[string]bool)
	c.mapTasksCompleted = make(map[string]bool)
	c.mapJobAssignments = make(map[string]int)

	for i, file := range files {
		// populate mapJobs and mapTasksRemaining
		c.mapJobs[file] = i
		c.mapJobAssignments[file] = 0
		c.mapTasksRemaining = append(c.mapTasksRemaining, file)

	}

	// populate reduce variables
	for i := 0; i < nReduce; i++ {
		c.reduceTasksRemaining = append(c.reduceTasksRemaining, i)
	}
	c.reduceTasksInProgress = make(map[int]bool)
	c.reduceTasksCompleted = make(map[int]bool)

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
