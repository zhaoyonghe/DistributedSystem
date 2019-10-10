package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  viewnum uint
  stMap map[string] string
  uidMap map[int64] string
  vshost string
  mu sync.Mutex
  newServer bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  view, _ := pb.vs.Get()
  if view.Primary == pb.me {
  	// I am primary
  	_, doneBefore := pb.uidMap[args.UID]
  	if !doneBefore {
  		// This put operation has not been done before.
	 		if args.DoHash {
	 			// This operation is PutDoHash.
				//fmt.Println("put dohash")
				value, exists := pb.stMap[args.Key]
				if !exists {
					value = ""
				}
				pb.stMap[args.Key] = strconv.Itoa(int(hash(value + args.Value)))
				pb.uidMap[args.UID] = value
				reply.PreviousValue = value
				reply.Err = OK

				if view.Backup != "" {
					ok := call(view.Backup, "PBServer.BackupPut", args, &reply)
					fmt.Printf("backup put dohash %t\n", ok)
				}
			} else {
				// This operation is Put.
				//fmt.Println("put")
				pb.stMap[args.Key] = args.Value
				pb.uidMap[args.UID] = ""
				reply.Err = OK
				
				if view.Backup != "" {
					ok := call(view.Backup, "PBServer.BackupPut", args, &reply)
					fmt.Printf("backup put %t\n", ok)
				}
			}
  	} else {
			// This put operation has been done before.
			// Directly give back the result.
  		fmt.Println("this put operation has done before!!!!!!!!!")
  		if args.DoHash {
  			reply.PreviousValue = pb.uidMap[args.UID]
  			reply.Err = OK
  		} else {
  			reply.Err = OK
  		}
  	}
  } else {
  	// I am backup
  	fmt.Println("i am backup!!!!!!!!!")
  	reply.Err = ErrWrongServer
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  primary := pb.vs.Primary()
  if primary == pb.me {
		value, exists := pb.stMap[args.Key]
		//fmt.Println("get")
		if exists {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
  } else {
  	// I am backup
  	fmt.Println("i am backup!!!!!!!!!")
  	reply.Err = ErrWrongServer
  	reply.Value = ""
  }
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  args := &viewservice.PingArgs{}
  args.Me = pb.me
  args.Viewnum = pb.viewnum
  
  var reply viewservice.PingReply

  // send an RPC request, wait for the reply.
  ok := call(pb.vshost, "ViewServer.Ping", args, &reply)
  
  pb.viewnum = reply.View.Viewnum
	//fmt.Printf("ping %d %t\n", pb.viewnum, ok)
	
	if reply.View.Primary == pb.me && reply.View.Backup != "" {
		// i am the primary and has a backup
		// check if that backup is new (needs stMap)
		cknArgs := &CheckNewArgs{}
		
		var cknReply CheckNewReply
		
		ok = call(reply.View.Backup, "PBServer.Check", cknArgs, &cknReply)
		if cknReply.New {
			// is new
			// transfer the stMap to backup
			tmArgs := &TransferMapArgs{}
			
			targetMap := make(map[string] string)
			for key, value := range pb.stMap {
				targetMap[key] = value
			}
			tmArgs.StMap = targetMap
			
			targetMap2 := make(map[int64] string)
			for key, value := range pb.uidMap {
				targetMap2[key] = value
			}
			tmArgs.UIDMap = targetMap2
			
			var tmReply TransferMapReply
			ok = call(reply.View.Backup, "PBServer.TransferMap", tmArgs, &tmReply)
			if !ok || !tmReply.Received {
				fmt.Println("Transfer failed!!!!!!")
			} else {
				fmt.Println("Transfer successed ***********")
			}
		}
	}
}

// check if the backup is new (needs stMap)
func (pb *PBServer) Check(args *CheckNewArgs, reply *CheckNewReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

	if pb.newServer {
		fmt.Println("I am new backup.................")
		reply.New = true
	} else {
		fmt.Println("I am not new backup!")
		reply.New = false
	}
	return nil
}


// transfer the whole database(stMap) to the backup
func (pb *PBServer) TransferMap(args *TransferMapArgs, reply *TransferMapReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

	view, _ := pb.vs.Get()
	
	if view.Backup == pb.me {
		// i am the backup, receive the map
		pb.stMap = args.StMap
		pb.uidMap = args.UIDMap
		pb.newServer = false
		reply.Received = true
		return nil
	}

	return fmt.Errorf("TransferMap: I am primary %s!", view.Backup)
}

// put the same key-value pair to the backup
func (pb *PBServer) BackupPut(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

	view, _ := pb.vs.Get()
	if pb.me == view.Backup {
  	// I am backup
		if args.DoHash {
			fmt.Println("backupput dohash")
			value, exists := pb.stMap[args.Key]
			if !exists {
				value = ""
			}
			pb.stMap[args.Key] = strconv.Itoa(int(hash(value + args.Value)))
			pb.uidMap[args.UID] = value
			reply.PreviousValue = value
			reply.Err = OK

		} else {
			fmt.Println("backupput")
			pb.stMap[args.Key] = args.Value
			pb.uidMap[args.UID] = ""
			reply.Err = OK
		}
		return nil
	}
	return fmt.Errorf("BackupPut: I am primary %s!", view.Backup)
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.viewnum = 0
  pb.stMap = make(map[string] string)
  pb.uidMap = make(map[int64] string)
  pb.vshost = vshost
  pb.newServer = true

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
