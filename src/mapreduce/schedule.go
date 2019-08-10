package mapreduce

import (
	"fmt"
	"log"
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

	/// 1. Get registered workers's RPC address from registerChan.
	/// 2. Send RPCs to the workers concurrently.
	/// 3. Use channel wait for a worker to finish before it can give it another task.
	/// 4. Use sync.Watigroup to wait all goroutine done.

	var (
		wg          sync.WaitGroup
		idleWorkers = make(chan string, 1<<10)
	)

	go func() {
		for {
			workerAddr := <-registerChan

			select {
			case idleWorkers <- workerAddr:
			default:
				fmt.Printf("too many workers, drop worker %s\n", workerAddr)
			}
		}
	}()

	for i := 0; i < ntasks; i++ {
		var file string
		if phase == mapPhase {
			file = mapFiles[i]
		}
		args := &DoTaskArgs{
			JobName:       jobName,
			File:          file,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			workerAddr := <-idleWorkers
			ok := call(workerAddr, "Worker.DoTask", args, new(struct{}))
			if ok == false {
				log.Fatalf("DoTask: RPC %s DoTask error", workerAddr)
			}
			idleWorkers <- workerAddr
		}()
	}

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
