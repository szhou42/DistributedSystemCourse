package mapreduce

import (
	"fmt"
	"sync"
	"time"
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

/*
	Plan:
		Take 1 registered worker off registerChan as long as we have task to hand out(i < ntasks)
		Ask the worker to do work:
			1) make a blocking rpc call
			2) put the worker back to registerChan
*/

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
	var wg sync.WaitGroup
	i := 0
	for i < ntasks {
		wg.Add(1)
		go func(jobName string, file string, phase jobPhase, task_number int, n_other int, ch chan string, wg *sync.WaitGroup) {
			for true {
				addr :=<- registerChan
				success := call(addr, "Worker.DoTask", DoTaskArgs{
					JobName: jobName,
					File: file,
					Phase: phase,
					TaskNumber: task_number,
					NumOtherPhase: n_other,
				}, nil)

				// Run a go routine to put the failed/successful worker back to channel
				go func() { ch <- addr }()
				if success {
					break
				}
				time.Sleep(200)
			}
			wg.Done()
		}(jobName, mapFiles[i], phase, i, n_other, registerChan, &wg)
		i += 1
	}
	wg.Wait()
}