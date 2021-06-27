package mr

import (
	"fmt"
	"testing"
)

func TestTaskList(t *testing.T) {
	t.Run("TestTaskList", func(t *testing.T) {
		list := &TaskList{
			Size: 0,
		}
		if res := list.get(1); res != nil {
			t.Errorf("TaskList.getTask() = %v, want %v", res, nil)
		}
		task1 := &Task{
			Type: 0,
			ID:   1,
		}
		task2 := &Task{
			Type: 0,
			ID:   2,
		}
		task3 := &Task{
			Type: 0,
			ID:   3,
		}
		list.addLast(task1)
		if res := list.get(1); res != task1 {
			t.Errorf("TaskList.getTask() = %v, want %v", res, task1)
		}
		if res := list.remove(1); res != task1 {
			t.Errorf("TaskList.getTask() = %v, want %v", res, task1)
		}
		list.addLast(task2)
		list.addLast(task1)
		list.addLast(task3)
		list.remove(1)
		if size := list.size(); size != 2 {
			t.Errorf("TaskList size = %v, want %v", size, 2)
		}
		p := list.Head
		names := []int{
			2,
			3,
		}
		for _, name := range names {
			if p == nil || p.Task.ID != name {
				t.Errorf("name = %v, want %v", p.Task.ID, name)
			}
			p = p.Next
		}
		namesReversed := []int{
			3,
			2,
		}
		p = list.Tail
		for _, name := range namesReversed {
			fmt.Println(p.Task.ID)
			if p == nil || p.Task.ID != name {
				t.Errorf("name = %v, want %v", p.Task.ID, name)
			}
			p = p.Pre
		}
		if res := list.get(3); res != task3 {
			t.Errorf("getTask() = %v, want %v", res, task3)
		}
		if res := list.get(4); res != nil {
			t.Errorf("getTask() = %v, want %v", res, nil)
		}
		list.remove(4)
		list.remove(2)
		list.remove(3)
		list.remove(2)
		if list.Head != nil || list.Tail != nil || list.Size != 0 {
			t.Errorf("head = %v, tail = %v, size = %v, want head = %v, tail = %v, size = %v",
				list.Head, list.Tail, list.Size, nil, nil, 0)
		}
	})
}
