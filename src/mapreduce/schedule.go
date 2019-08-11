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

	/// 1. Get registered workers's RPC address from registerChan.
	/// 2. Send RPCs to the workers concurrently.
	/// 3. Use channel wait for a worker to finish before it can give it another task.
	/// 4. Use sync.Watigroup to wait all goroutine done.

	var wg sync.WaitGroup

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
			for {
				workerAddr := <-registerChan
				ok := call(workerAddr, "Worker.DoTask", args, new(struct{}))
				if ok == true {
					go func() { registerChan <- workerAddr }()
					break
				}

				fmt.Printf("DoTask: RPC %s DoTask error, will be sheduled to other worker\n", workerAddr)
			}
		}()
	}

	wg.Wait()

	go func() {
		for {
			// Avoiding other goroutine stuck on registerChan,
			// such as forwardRegistrations() or our go func() above.
			<-registerChan
			fmt.Println("RegisterChan: drop one worker")
		}
	}()

	fmt.Printf("Schedule: %v done\n", phase)
}
