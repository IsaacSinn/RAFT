package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Call task to get a task from the coordinator
		task := requestTask()
		taskType := task.TaskType

		if taskType == "map" {
			fileName := task.Filename
			NReduce := task.NReduce
			JobID := task.JobID
			AssignmentCount := task.AssignmentCount

			file, err := os.Open(fileName)
			if err != nil {
				log.Fatalf("cannot open %v", fileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", fileName)
			}
			file.Close()
			kva := mapf(fileName, string(content))

			// create a map to store temp intermediate files
			intermediate_files := make(map[string]*json.Encoder)
			temp_files_names := make([]string, NReduce)

			//loops from 1 to NReduce and create all the buckets
			for i := 0; i < NReduce; i++ {
				intermediate_file_name := fmt.Sprintf("temp-%d-%d-%d", JobID, AssignmentCount, i)

				// append the file name to the temp_files_names
				temp_files_names = append(temp_files_names, intermediate_file_name)

				intermediate_file, err := os.Create(intermediate_file_name)

				if err != nil {
					log.Fatalf("cannot create %v", intermediate_file_name)
				}
				intermediate_files[intermediate_file_name] = json.NewEncoder(intermediate_file)
			}

			// loop through each kv pair store it into an respective intermediate file
			for _, kv := range kva {
				reduce_num := ihash(kv.Key) % NReduce
				intermediate_file_name := fmt.Sprintf("temp-%d-%d-%d", JobID, AssignmentCount, reduce_num)
				intermediate_files[intermediate_file_name].Encode(&kv)
			}

			reportTaskDoneMap(fileName, JobID, taskType, temp_files_names, AssignmentCount)

		} else if taskType == "reduce" {

			JobID := task.JobID

			// fmt.Printf("JobID %d\n", JobID)

			totalMapJobs := task.TotalMapJobs

			kva := []KeyValue{}

			// read the intermediate files for that partition
			for i := 0; i < totalMapJobs; i++ {
				tempFileName := fmt.Sprintf("mr-%d-%d", i, JobID)
				tempFile, err := os.Open(tempFileName)
				if err != nil {
					log.Fatalf("cannot open %v", tempFileName)
				}

				dec := json.NewDecoder(tempFile)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("temp-out-%d", JobID)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()

			// fmt.Printf("Task Done: %d finished \n", JobID)

			reportTaskDoneReduce(oname, JobID, taskType)
		}

		// time.Sleep(4 * time.Second)

	}

}

func requestTask() *RequestTaskReply {
	// declare an argument structure.
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &args, &reply)

	return &reply

}

func reportTaskDoneMap(fileName string, taskID int, taskType string, temp_files_names []string, AssignmentCount int) {
	args := ReportTaskDoneArgs{
		FileName:        fileName,
		TaskID:          taskID,
		TaskType:        taskType,
		TempFilesNames:  temp_files_names,
		AssignmentCount: AssignmentCount,
	}
	reply := ReportTaskDoneReply{}

	call("Coordinator.ReportTaskDone", &args, &reply)

}

func reportTaskDoneReduce(fileName string, taskID int, taskType string) {
	args := ReportTaskDoneArgs{
		FileName: fileName,
		TaskID:   taskID,
		TaskType: taskType,
	}
	reply := ReportTaskDoneReply{}

	call("Coordinator.ReportTaskDone", &args, &reply)

}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
