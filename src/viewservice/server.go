
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "container/list"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
	curView View
	acked bool
	timeMap map[string] time.Time
	idleQueue *list.List
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock();
  defer vs.mu.Unlock()
  
  fmt.Printf("before: curView: %v\n", vs.curView)
  
  _, ok := vs.timeMap[args.Me]
  if ok {
  	// old clerk
  	
  	// update the time, continue the life
  	vs.timeMap[args.Me] = time.Now()
  	
  	// check the type of this clerk
  	if args.Me == vs.curView.Primary {
  		// this clerk is the primary
  		if args.Viewnum == 0 && vs.curView.Viewnum != 0 {
  			// the primary crashes and quickly restarts without missing sending a single Ping.
  			primaryDied(vs, args.Me)
  		} else if args.Viewnum == 0 && vs.curView.Viewnum == 0 {
  			// this will never happen
  			fmt.Println("================================")
  			fmt.Println("never hapen")
  			fmt.Println("================================")
  		} else if args.Viewnum == vs.curView.Viewnum {
  			vs.acked = true
  		}
  	} else if args.Me == vs.curView.Backup {
  		// this clerk is the backup
  		if args.Viewnum == 0 && vs.curView.Viewnum != 0 {
  			// the backup crashes and quickly restarts without missing sending a single Ping.
  			backupDied(vs, args.Me)
  		}
  		// if args.Viewnum <= vs.curView.Viewnum
  		// normal, do nothing
  	} else {
  		// this clerk is an idle clerk
  		// do nothing
  	}
  	reply.View = vs.curView
  } else {
  	// new clerk
  	
  	// add this clerk into the map
  	vs.timeMap[args.Me] = time.Now()
  	
  	// check the current view
  	if vs.curView.Primary == "" && vs.curView.Backup == "" {
  		// no primary and backup
  		// the new clerk will become a primary
  		vs.curView.Viewnum += 1
  		vs.curView.Primary = args.Me
  		
  		// the view has changed
  		// need to be acked by primary
  		vs.acked = false
  		
  		reply.View = vs.curView
  	} else if vs.curView.Primary != "" && vs.curView.Backup == "" {
  		// no backup but has primary
  		
  		if vs.acked {
  			// acked
  			// view can be changed
  			vs.curView.Viewnum += 1
  			vs.curView.Backup = args.Me
  			
				// the view has changed
				// need to be acked by primary
  			vs.acked = false
  			
  			reply.View = vs.curView
  		} else {
  			// not acked 
  			fmt.Println("***********************************")
  			fmt.Println("***********************************")
  			fmt.Println("***********************************")
  			fmt.Println("***********************************")
  			fmt.Println("***********************************")
  			fmt.Println("***********************************")
  			reply.View = vs.curView
  			vs.curView.Viewnum += 1
  			vs.curView.Backup = args.Me
  		}
  	} else if vs.curView.Primary != "" && vs.curView.Backup != "" {
  		// has primary and backup
  		// this clerk will become an idle clerk
  		vs.idleQueue.PushBack(args.Me)
  		
  		// view should not change
  		reply.View = vs.curView
  	} else {
			// this will never happen
			fmt.Println("================================")
			fmt.Println("never hapen")
			fmt.Println("================================")
  	}
  }
  
  fmt.Printf("after: curView: %v\n", vs.curView)

  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
	reply.View = vs.curView
	
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()
  
  for clerkName, clerkLastTime := range vs.timeMap {
  	if time.Since(clerkLastTime) > DeadPings * PingInterval {
  		// clerkName has died
  		if clerkName == vs.curView.Primary {
  			if vs.acked {
  				// acked
  				// cannot change the view
  				primaryDied(vs, clerkName)
  			}
  		} else if clerkName == vs.curView.Backup {
  			if vs.acked {
  				// acked
  				// cannot change the view
  				backupDied(vs, clerkName)
  			}
  		} else {
  			idleDied(vs, clerkName)
  		}
  	}
  }
}


func primaryDied(vs *ViewServer, clerkName string) {
	fmt.Println("Primary Died")
	// primary has died
	// promote the backup as the primary
	vs.curView.Primary = vs.curView.Backup
	
	if vs.idleQueue.Len() == 0 {
		vs.curView.Backup = ""
	} else {
		// if there is an idle server, promote it as a backup
		vs.curView.Backup = vs.idleQueue.Front().Value.(string)
		vs.idleQueue.Remove(vs.idleQueue.Front())
	}
	
	vs.curView.Viewnum += 1
	vs.acked = false
	delete(vs.timeMap, clerkName)
	
	// situation that we have the primary, do not have the backup, but have the idle servers does not exist
	// so this function will never cause a view that has no primary but has backup
}

func backupDied(vs *ViewServer, clerkName string) {
	fmt.Println("Backup Died")
	
	if vs.idleQueue.Len() == 0 {
		vs.curView.Backup = ""
	} else {
		// if there is an idle server, promote it as a backup
		vs.curView.Backup = vs.idleQueue.Front().Value.(string)
		vs.idleQueue.Remove(vs.idleQueue.Front())
	}
	
	vs.curView.Viewnum += 1
	vs.acked = false
	delete(vs.timeMap, clerkName)
}

func idleDied(vs *ViewServer, clerkName string) {
	var ele *list.Element = nil
	for e := vs.idleQueue.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == clerkName {
			ele = e
			break;
		}
	}
	
	if ele != nil {
		vs.idleQueue.Remove(ele)
	}
}


//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.dead = false
  vs.curView = View {0, "", ""};
  vs.timeMap = make(map[string] time.Time)
  vs.acked = false
  vs.idleQueue = list.New()
  

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
