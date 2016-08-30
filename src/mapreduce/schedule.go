package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	done := make(chan bool)

	for i := 0; i< ntasks ;i++ {
		go func(nthTask int) {
			var worker string
			reply := new(struct{})
			args := DoTaskArgs{mr.jobName, mr.files[nthTask], phase, nthTask, nios}
			ok := false
			for ok!=true {
				worker = <- mr.registerChannel
				ok = call(worker, "Worker.DoTask", args, reply)
			}
			// 'done' must be before 'registerChannel'
			//  otherwise, master is still in current phase
			//  and no one wants to get worker from mr.registerChannel
			//  then everything stucks here
			done <- true
			mr.registerChannel <- worker
		}(i)
	}


	// wait for task completion
	// otherwise, master exits and workss are still doing tasks
	for i := 0; i< ntasks; i++ {
		<- done
	}


	fmt.Printf("Schedule: %v phase done\n", phase)
}
