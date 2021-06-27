package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MAPPING  int8 = 1 << 1
	REDUCING int8 = 1 << 2
	DONE     int8 = 1 << 3
)

type Coordinator struct {
	// Your definitions here.
	Status         int8
	NReduce        int
	GlobalWorkerId int64
	Waitting       *TaskList
	Running        *TaskList
	Finished       *TaskList
}

type Task struct {
	Type           byte
	ID             int
	FileNamePrefix string
	WorkerId       int64
	StartTime      int64
	EndTime        int64
}

type Req struct {
	TaskID   int
	WorkerId int64
}

type Rsp struct {
	Type           byte
	TaskID         int
	FileNamePrefix string
	NReduce        int
	WorkerId       int64
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AskForTask(req *Req, rsp *Rsp) error {
	doneID := req.TaskID
	if req.WorkerId == -1 {
		req.WorkerId = c.GlobalWorkerId
		c.GlobalWorkerId++
	}
	if doneID != -1 {
		task := c.Running.get(doneID)
		fmt.Printf("running %v\n", task)
		if task != nil && task.WorkerId == req.WorkerId {
			doneTask := c.Running.remove(doneID)
			fmt.Printf("done %v\n", task)
			if doneTask != nil {
				doneTask.EndTime = nowMillUTC()
				c.Finished.addLast(doneTask)
				c.commitFile(doneID, doneTask.WorkerId)
			}
		}
	}
	if c.Waitting.size() == 0 && c.Running.size() == 0 {
		if c.Status == MAPPING {
			c.Status = REDUCING
			for i := 0; i < c.NReduce; i++ {
				task := &Task{
					Type: 1,
					ID:   i,
				}
				c.Waitting.addLast(task)
				fmt.Printf("add reduce task: %v\n", task)
			}
		} else {
			c.Status = DONE
		}
	}
	if c.Status != DONE {
		task := c.Waitting.removeFirst()
		task.StartTime = nowMillUTC()
		task.WorkerId = req.WorkerId
		c.Running.addLast(task)
		if c.Status == MAPPING {
			rsp.Type = 0
		} else {
			rsp.Type = 1
		}
		rsp.TaskID = task.ID
		rsp.FileNamePrefix = task.FileNamePrefix
		rsp.NReduce = c.NReduce
		rsp.WorkerId = task.WorkerId
	}
	return nil
}

func (c *Coordinator) commitFile(taskID int, workerId int64) {
	var temp string
	var real string
	switch c.Status {
	case MAPPING:
		for i := 0; i < c.NReduce; i++ {
			// e.g. mr-1-1-1
			temp = fmt.Sprintf("mr-%d-%d-%d", taskID, i, workerId)
			real = fmt.Sprintf("mr-%d-%d", taskID, i)
			os.Rename(temp, real)
		}
	case REDUCING:
		// e.g. mr-out-1-1
		temp = fmt.Sprintf("mr-out-%d-%d", taskID, workerId)
		real = fmt.Sprintf("mr-out-%d", taskID)
		os.Rename(temp, real)
	default:
	}
	fmt.Printf("commit file %s to %s\n", temp, real)
}

func (c *Coordinator) scanTaskBG() {

}

func nowMillUTC() int64 {
	return time.Now().UTC().Unix()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.Status == DONE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	watting := &TaskList{
		Size: 0,
	}
	running := &TaskList{
		Size: 0,
	}
	finished := &TaskList{
		Size: 0,
	}

	c := Coordinator{
		Status:         MAPPING,
		NReduce:        nReduce,
		GlobalWorkerId: 0,
		Waitting:       watting,
		Running:        running,
		Finished:       finished,
	}

	for i, file := range files {
		task := &Task{
			Type:           0,
			ID:             i,
			FileNamePrefix: file,
		}
		c.Waitting.addLast(task)
	}
	c.server()
	return &c
}

//
// start a thread that listens for RPCs from worker.go
//
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

type TaskNode struct {
	Task *Task
	Pre  *TaskNode
	Next *TaskNode
}

type TaskList struct {
	Head *TaskNode
	Tail *TaskNode
	Size int
}

func (t *TaskList) get(id int) *Task {
	node := t.Head
	for node != nil {
		task := node.Task
		if id == task.ID {
			return task
		}
		node = node.Next
	}
	return nil
}

func (t *TaskList) remove(id int) *Task {
	node := t.Head
	if node == nil {
		return nil
	}
	for node != nil {
		task := node.Task
		if task.ID == id {
			left := node.Pre
			right := node.Next
			if left != nil {
				left.Next = right
			} else {
				t.Head = right
			}
			if right != nil {
				right.Pre = left
			} else {
				t.Tail = left
			}
			t.Size--
			return task
		}
		node = node.Next
	}
	return nil
}

func (t *TaskList) addLast(task *Task) {
	node := t.Head
	newNode := &TaskNode{
		Task: task,
	}
	if node == nil {
		t.Head = newNode
		t.Tail = t.Head
	} else {
		t.Tail.Next = newNode
		newNode.Pre = t.Tail
		t.Tail = newNode
	}
	t.Size++
}

func (t *TaskList) removeFirst() *Task {
	if t.Head == nil {
		return nil
	}
	res := t.Head.Task
	t.Head = t.Head.Next
	if t.Head == nil {
		t.Tail = nil
	} else {
		t.Head.Pre = nil
	}
	t.Size--
	return res
}

func (t *TaskList) size() int {
	return t.Size
}
