package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	worker := make(chan string)
	go func() {
		for w := range mr.registerChannel {
			mr.Workers[w] = &WorkerInfo{w}
			go func(w string) {
				worker <- w
			}(w)
		}
	}()
	doJob := func(op JobType, nJobs, nOtherJobs int) {
		active, done := make(chan int), make(chan bool)
		go func() {
			for i := 0; i < nJobs; i++ {
				active <- i
			}
		}()
		go func() {
			for i := range active {
				go func(i int) {
					w := <-worker
					var reply DoJobReply
					ok := call(w, "Worker.DoJob", &DoJobArgs{mr.file, op, i, nOtherJobs}, &reply)
					if ok == true {
						done <- true
					} else {
						active <- i
					}
					worker <- w
				}(i)
			}
		}()
		for i := 0; i < nJobs; i++ {
			<-done
		}
	}
	doJob(Map, mr.nMap, mr.nReduce)
	doJob(Reduce, mr.nReduce, mr.nMap)
	return mr.KillWorkers()
}
