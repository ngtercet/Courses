package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		doTaskArgs := DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[i],
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other}
		go func() {
			worker := <-registerChan
			fmt.Println("=====================" + worker)
			for !call(worker, "Worker.DoTask", doTaskArgs, nil) {
				worker = <-registerChan
			}
			wg.Done()
			// 注意这里的registerChan是一个无缓存的通道
			// 每次往其中加入worker时会阻塞到有消费者取走此worker
			// 所以最后一个任务线程结束时会阻塞在此处
			// 如果放回worker的操作在wg.Done()前执行wg的值会阻塞在1，导致主线程退出不了
			registerChan <- worker

		}()
	}
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
