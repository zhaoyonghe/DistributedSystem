package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
  reply *DoJobReply
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

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  //nTotal = nMap + nReduce
  
  mr.Workers = make(map[string] *WorkerInfo)
  
	wk0 := <- mr.registerChannel
	wk1 := <- mr.registerChannel
	
	
	mr.Workers["0"] = &WorkerInfo{address: wk0}
	mr.Workers["1"] = &WorkerInfo{address: wk1}
	
	//fmt.Printf("%v", mr.Workers["0"]);
	
	// do map first
	
	for i := 0; i < nMap; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.Operation = "Map"
		args.JobNumber = i
		args.NumOtherPhase = mr.nReduce
		fmt.Printf("%v\n", *args);
		
		var reply DoJobReply
		
		for {
			for _, wkinfo := range mr.Workers {
				//fmt.Printf("%v\n", mr.Workers[k]);
				if wkinfo.reply == nil || wkinfo.reply.OK {
					wkinfo.reply = &reply
					ok := call(wkinfo.address, "Worker.DoJob", args, &reply)
					if ok {
						fmt.Println("maphaha")
					} else {
						fmt.Println("mapfuck")
					}
					goto end
				}
			}
		}
		
		end:
	}

  // do reduce
  
	for i := 0; i < nReduce; i++ {
		args := &DoJobArgs{}
		args.File = mr.file
		args.Operation = "Reduce"
		args.JobNumber = i
		args.NumOtherPhase = mr.nMap
		fmt.Printf("%v\n", *args);
		
		var reply DoJobReply
		
		for {
			for _, wkinfo := range mr.Workers {
				//fmt.Printf("%v\n", mr.Workers[k]);
				if wkinfo.reply == nil || wkinfo.reply.OK {
					wkinfo.reply = &reply
					ok := call(wkinfo.address, "Worker.DoJob", args, &reply)
					if ok {
						fmt.Println("reducehaha")
					} else {
						fmt.Println("reducefuck")
					}
					goto end1
				}
			}
		}
		
		end1:
	} 
	
  return mr.KillWorkers()
}
