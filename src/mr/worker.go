package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	for {
		t := getTask()
		log.Printf("Get task with type = %v, order = %v.\n", t.TaskType, t.TaskOrder)

		switch t.TaskType {
		case "completed":
			return
		case "waiting_maps":
			time.Sleep(3 * time.Second)
			continue
		case "waiting_reduces":
			time.Sleep(3 * time.Second)
			continue
		case "map":
			doMapTask(mapf, t)
			submitTask(t)
			continue
		case "reduce":
			doReduceTask(reducef, t)
			submitTask(t)
			continue
		default:
			log.Fatal("Unknown task type: ", t.TaskType)
		}
	}
}

func getTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	for i := 0; i < 3; i++ {
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			return reply
		}
	}
	log.Fatal("Failed to get task.")
	return GetTaskReply{}
}

func submitTask(t GetTaskReply) {
	log.Printf("Submitting task with type = %v, order = %v.\n", t.TaskType, t.TaskOrder)
	args := SubmitTaskArgs{t.TaskType, t.TaskOrder}
	reply := SubmitTaskReply{}
	for i := 0; i < 3; i++ {
		ok := call("Coordinator.SubmitTask", &args, &reply)
		if ok {
			return
		}
	}
	log.Fatal("Failed to submit task. (type " + t.TaskType + ", order " + strconv.Itoa(t.TaskOrder) + ")")
}

func doMapTask(mapf func(string, string) []KeyValue, t GetTaskReply) {
	log.Printf("Start map task with order = %v.\n", t.TaskOrder)

	content, err := os.ReadFile(t.Filename)
	if err != nil {
		log.Fatal(err)
	}

	kva := mapf(t.Filename, string(content))

	var intermediates []*json.Encoder
	for i := 0; i < t.Nreduce; i++ {
		f, err := os.Create("mr-" + strconv.Itoa(t.TaskOrder) + "-" + strconv.Itoa(i))
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(f)
		intermediates = append(intermediates, enc)
	}

	for _, kv := range kva {
		n := ihash(kv.Key) % t.Nreduce
		err := intermediates[n].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func doReduceTask(reducef func(string, []string) string, t GetTaskReply) {
	log.Printf("Start reduce task with order = %v.\n", t.TaskOrder)

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	items, err := os.ReadDir(pwd)
	if err != nil {
		log.Fatal(err)
	}

	intermediates := make(map[string][]string)
	for _, item := range items {
		if item.IsDir() {
			continue
		}

		pattern := `^mr-\d+-` + strconv.Itoa(t.TaskOrder)
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Fatal(err)
		}
		if re.MatchString(item.Name()) {
			f, err := os.Open(item.Name())
			if err != nil {
				log.Fatal(err)
			}
			dec := json.NewDecoder(f)
			for {
				kv := KeyValue{}
				err := dec.Decode(&kv)
				if err != nil {
					break
				}
				intermediates[kv.Key] = append(intermediates[kv.Key], kv.Value)
			}
		}
	}

	f, err := os.CreateTemp(pwd, "temp-mr-out-")
	if err != nil {
		log.Fatal(err)
	}
	for key, values := range intermediates {
		output := reducef(key, values)
		fmt.Fprintf(f, "%v %v\n", key, output)
	}
	e := os.Rename(f.Name(), filepath.Join(pwd, "mr-out-"+strconv.Itoa(t.TaskOrder)))
	if e != nil {
		log.Fatal(err)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
