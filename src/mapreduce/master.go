package mapreduce
import "container/list"
import "fmt"
import "sync"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  key string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func asynCall(phase string, wkinfo *WorkerInfo, idleWorkers chan string, failedWorkers chan string, mr *MapReduce, jobsQueue *list.List, jobsQueueMutex *sync.Mutex, jobsCount *int, jobsCountMutex *sync.Mutex) {
	jn := popJobsQueue(jobsQueue, jobsQueueMutex)
	if(jn != -1) {
		args := &DoJobArgs{}
		var reply DoJobReply
		
		args.File = mr.file
		args.JobNumber = jn
		
		if(phase == "Map"){
			args.Operation = "Map"
			args.NumOtherPhase = mr.nReduce
		} else {
			args.Operation = "Reduce"
			args.NumOtherPhase = mr.nMap
		}
			
		ok := call(wkinfo.address, "Worker.DoJob", args, &reply)
		
		if ok {
			idleWorkers <- wkinfo.key
			decJobsCount(jobsCount, jobsCountMutex)
		} else {
			failedWorkers <- wkinfo.key
			pushJobsQueue(jobsQueue, jn, jobsQueueMutex)
		}
	} else {
		idleWorkers <- wkinfo.key
	}
}

func getJobsCount(jobsCount *int, jobsCountMutex *sync.Mutex) int {
	jobsCountMutex.Lock()
	defer jobsCountMutex.Unlock()
	var jc int = *jobsCount
	return jc
}

func decJobsCount(jobsCount *int, jobsCountMutex *sync.Mutex) {
	jobsCountMutex.Lock()
	defer jobsCountMutex.Unlock()
	*jobsCount -= 1
}

func pushJobsQueue(jobsQueue *list.List, jobNum int, jobsQueueMutex *sync.Mutex) {
	jobsQueueMutex.Lock()
	defer jobsQueueMutex.Unlock()
	jobsQueue.PushBack(jobNum)
}

func popJobsQueue(jobsQueue *list.List, jobsQueueMutex *sync.Mutex) int {
	jobsQueueMutex.Lock()
	defer jobsQueueMutex.Unlock()
	if jobsQueue.Len() == 0 {
		return -1
	}
	jobNum := jobsQueue.Front().Value.(int)
	jobsQueue.Remove(jobsQueue.Front())
	return jobNum
}

func getMap(workers map[string] *WorkerInfo, key string, mapMutex *sync.Mutex) *WorkerInfo {
	mapMutex.Lock()
	defer mapMutex.Unlock()
	var wkinfo *WorkerInfo = workers[key]
	return wkinfo
}

func setMap(workers map[string] *WorkerInfo, key string, wkinfo *WorkerInfo, mapMutex *sync.Mutex) {
	mapMutex.Lock()
	defer mapMutex.Unlock()
	workers[key] = wkinfo
}

func detectWorkerFailure(failedWorkers chan string, idleWorkers chan string, mr *MapReduce, mapMutex *sync.Mutex) {
	for {
		var key string
		select {
		case key = <- failedWorkers:
			setMap(mr.Workers, key, &WorkerInfo{address: <- mr.registerChannel, key: key}, mapMutex)
			idleWorkers <- key
		default:
		}
	}
}


func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  
  mr.Workers = make(map[string] *WorkerInfo)
  var mapMutex sync.Mutex
  
  idleWorkers := make(chan string, 2)
  failedWorkers := make(chan string, 2)

  wk0 := <- mr.registerChannel
	wk1 := <- mr.registerChannel

	mr.Workers["0"] = &WorkerInfo{address: wk0, key: "0"}
	mr.Workers["1"] = &WorkerInfo{address: wk1, key: "1"}
	
	idleWorkers <- "0"
	idleWorkers <- "1"

	var jobsQueueMutex sync.Mutex  
	
	var jobsCount int
	var jobsCountMutex sync.Mutex
	
	go detectWorkerFailure(failedWorkers, idleWorkers, mr, &mapMutex)
	
	// do map first
	
	mapJobsQueue := list.New()
	for i := 0; i < nMap; i++ {
		mapJobsQueue.PushBack(i)
	}
	
	jobsCount = nMap
	
	for {
		jc := getJobsCount(&jobsCount, &jobsCountMutex)
		if jc == 0 {
			break
		} else {
			key := <- idleWorkers
			wkinfo := getMap(mr.Workers, key, &mapMutex)
			go asynCall("Map", wkinfo, idleWorkers, failedWorkers, mr, mapJobsQueue, &jobsQueueMutex, &jobsCount, &jobsCountMutex)
		}
	}
	
	// do reduce
	
	reduceJobsQueue := list.New()
	for i := 0; i < nReduce; i++ {
		reduceJobsQueue.PushBack(i)
	}
	
	jobsCount = nReduce
	
	for {
		jc := getJobsCount(&jobsCount, &jobsCountMutex)
		if jc == 0 {
			break
		} else {
			key := <- idleWorkers
			go asynCall("Reduce", mr.Workers[key], idleWorkers, failedWorkers, mr, reduceJobsQueue, &jobsQueueMutex, &jobsCount, &jobsCountMutex)
		}
	}

  return mr.KillWorkers()
}
