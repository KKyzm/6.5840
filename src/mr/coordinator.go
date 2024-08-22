package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mr_task struct {
	status     string
	ch         chan int
	retry_time int
	filename   string
	num_reduce int
}

type Coordinator struct {
	map_tasks    []mr_task
	reduce_tasks []mr_task
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	map_all_finished := true
	for idx := range c.map_tasks {
		map_task := &c.map_tasks[idx]
		switch map_task.status {
		case "unallocated":
			// allocate map task
			reply.TaskType = "map"
			reply.Filename = map_task.filename
			reply.TaskOrder = idx
			reply.Nreduce = map_task.num_reduce
			map_task.status = "pending"
			log.Printf("Map task assigned with order = %v.\n", idx)
			go c.supervise("map", idx)
			return nil
		case "pending":
			map_all_finished = false
		}
	}

	if map_all_finished {
		reduce_all_finished := true
		for idx := range c.reduce_tasks {
			reduce_task := &c.reduce_tasks[idx]
			switch reduce_task.status {
			case "unallocated":
				// allocate reduce task
				reply.TaskType = "reduce"
				reply.TaskOrder = idx
				reduce_task.status = "pending"
				log.Printf("Reduce task assigned with order = %v.\n", idx)
				go c.supervise("reduce", idx)
				return nil
			case "pending":
				reduce_all_finished = false
			}
		}

		if reduce_all_finished {
			reply.TaskType = "completed"
		} else {
			reply.TaskType = "waiting_reduces"
		}
		return nil
	} else {
		reply.TaskType = "waiting_maps"
		return nil
	}
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	log.Printf("Task submitted with type = %v, order = %v.\n", args.TaskType, args.TaskOrder)

	var mr_tasks *[]mr_task
	switch args.TaskType {
	case "map":
		mr_tasks = &c.map_tasks
	case "reduce":
		mr_tasks = &c.reduce_tasks
	default:
		log.Fatal("Unknown task type: " + args.TaskType)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for idx := range *mr_tasks {
		if idx == args.TaskOrder {
			if (*mr_tasks)[idx].status == "pending" {
				(*mr_tasks)[idx].status = "finished"
				(*mr_tasks)[idx].ch <- 0
			}
			break
		}
	}
	return nil
}

func (c *Coordinator) supervise(task_type string, task_order int) {
	var mr_tasks *[]mr_task
	switch task_type {
	case "map":
		mr_tasks = &c.map_tasks
	case "reduce":
		mr_tasks = &c.reduce_tasks
	default:
		log.Fatal("Unknown task type: " + task_type)
	}

	select {
	case <-(*mr_tasks)[task_order].ch:
		log.Printf("Task %v with order %v finished.\n", task_type, task_order)
		return
	case <-time.After(10 * time.Second):
		c.mu.Lock()
		(*mr_tasks)[task_order].status = "unallocated"
		log.Printf("Task %v with order %v timeout.\n", task_type, task_order)
		c.mu.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for idx := range c.reduce_tasks {
		if c.reduce_tasks[idx].status != "finished" {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	c := Coordinator{}

	for _, file := range files {
		c.map_tasks = append(c.map_tasks, mr_task{
			"unallocated",
			make(chan int),
			0,
			file,
			nReduce,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_tasks = append(c.reduce_tasks, mr_task{
			"unallocated",
			make(chan int),
			0,
			"", // redundant
			0,  // redundant
		})
	}

	// Your code here.

	c.server()
	return &c
}
