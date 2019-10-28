package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "math"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type Instance struct {
  seq int
  np int64
  na int64
  va interface{}
  decided bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  instanceMap map[int]*Instance
  peersDone []int
  pdmu sync.Mutex
}

func (px *Paxos) setPeersDone(i int, seq int) {
  px.pdmu.Lock()
  defer px.pdmu.Unlock()

  if px.peersDone[i] <= seq {
    px.peersDone[i] = seq
  } else {
    DPrintf("panic!!!!!!!!!!! px.peersDone[i]:%v, seq:%v\n", px.peersDone[i], seq)
    //panic("ds")
  }
}

func (px *Paxos) getPeersDone(i int) int {
  px.pdmu.Lock()
  defer px.pdmu.Unlock()

  return px.peersDone[i]
}

func (px *Paxos) sendPropose(seq int, N int64, i int, proposeReplyChan chan ProposeReply) {
  proposeArgs := &ProposeArgs{}
  var proposeReply ProposeReply

  proposeArgs.Seq = seq
  proposeArgs.N = N
  proposeArgs.MyDone = px.getPeersDone(px.me)
  proposeArgs.Me = px.me

  ok := call(px.peers[i], "Paxos.ProposeNHandler", proposeArgs, &proposeReply)
  DPrintf("%v: seq: %v got propose reply from %v: %v \n", px.me, seq, i, proposeReply)

  if ok {
    proposeReplyChan <- proposeReply
    px.setPeersDone(i, proposeReply.MyDone)
  } else {
    proposeReply.Message = NO_REPLY
    proposeReplyChan <- proposeReply
  }

}

func (px *Paxos) sendAccept(seq int, N int64, value interface{}, i int, acceptReplyChan chan AcceptReply) {
  acceptArgs := &AcceptArgs{}
  var acceptReply AcceptReply

  acceptArgs.Seq = seq
  acceptArgs.N = N
  acceptArgs.Val = value
  acceptArgs.MyDone = px.getPeersDone(px.me)
  acceptArgs.Me = px.me

  ok := call(px.peers[i], "Paxos.AcceptNVHandler", acceptArgs, &acceptReply)
  DPrintf("%v: seq: %v got propose reply from %v: %v \n", px.me, seq, i, acceptReply)

  if ok {
    acceptReplyChan <- acceptReply
    px.setPeersDone(i, acceptReply.MyDone)
  } else {
    acceptReply.Message = NO_REPLY
    acceptReplyChan <- acceptReply
  }
}

func (px *Paxos) sendDecide(seq int, value interface{}, i int) {
  decideArgs := &DecideArgs{}
  var decideReply DecideReply

  decideArgs.Seq = seq
  decideArgs.Val = value
  decideArgs.MyDone = px.getPeersDone(px.me)
  decideArgs.Me = px.me

  call(px.peers[i], "Paxos.DecideVHandler", decideArgs, &decideReply)
}

func (px *Paxos) ProposeNHandler(proposeArgs *ProposeArgs, proposeReply *ProposeReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if proposeArgs.Me != px.me {
    px.setPeersDone(proposeArgs.Me, proposeArgs.MyDone)
  }

  _, exists := px.instanceMap[proposeArgs.Seq]

  var inst *Instance

  if exists {
    inst = px.instanceMap[proposeArgs.Seq]
  } else {
    inst = &Instance{}
    inst.seq = proposeArgs.Seq
  }

  if inst.decided {
    proposeReply.Message = DECIDED
    proposeReply.Va = inst.va
    proposeReply.MyDone = px.getPeersDone(px.me)
    return nil
  }

  if proposeArgs.N > inst.np {
    inst.np = proposeArgs.N

    px.instanceMap[proposeArgs.Seq] = inst

    proposeReply.Message = PROPOSE_OK
    proposeReply.Na = inst.na
    proposeReply.Va = inst.va
    proposeReply.MyDone = px.getPeersDone(px.me)
  } else {
    proposeReply.Message = PROPOSE_REJECT
    proposeReply.Np = inst.np
    proposeReply.MyDone = px.getPeersDone(px.me)
  }

  return nil
}

func (px *Paxos) AcceptNVHandler(acceptArgs *AcceptArgs, acceptReply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if acceptArgs.Me != px.me {
    px.setPeersDone(acceptArgs.Me, acceptArgs.MyDone)
  }

  _, exists := px.instanceMap[acceptArgs.Seq]

  var inst *Instance

  if exists {
    inst = px.instanceMap[acceptArgs.Seq]
  } else {
    inst = &Instance{}
    inst.seq = acceptArgs.Seq
  }

  if inst.decided {
    acceptReply.Message = DECIDED
    acceptReply.Va = inst.va
    acceptReply.MyDone = px.getPeersDone(px.me)
    return nil
  }

  if acceptArgs.N >= inst.np {
    inst.np = acceptArgs.N
    inst.na = acceptArgs.N
    inst.va = acceptArgs.Val

    px.instanceMap[acceptArgs.Seq] = inst

    acceptReply.Message = ACCEPT_OK
    acceptReply.Np = acceptArgs.N
    acceptReply.MyDone = px.getPeersDone(px.me)
  } else {
    acceptReply.Message = ACCEPT_REJECT
    acceptReply.Np = inst.np
    acceptReply.MyDone = px.getPeersDone(px.me)
  }

  return nil
}


func (px *Paxos) DecideVHandler(decideArgs *DecideArgs, decideReply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if decideArgs.Me != px.me {
    px.setPeersDone(decideArgs.Me, decideArgs.MyDone)
  }

  _, exists := px.instanceMap[decideArgs.Seq]

  var inst *Instance

  if exists {
    inst = px.instanceMap[decideArgs.Seq]
  } else {
    inst = &Instance{}
    inst.seq = decideArgs.Seq
  }

  if !inst.decided {
    inst.decided = true
    inst.va = decideArgs.Val
  }

  DPrintf("%v decides on seq: %v, val: %v\n", px.me, decideArgs.Seq, inst.va)

  px.instanceMap[decideArgs.Seq] = inst

  return nil
}


func (px *Paxos) isDecided(seq int) bool {
  px.mu.Lock()
  defer px.mu.Unlock()

  inst, exists := px.instanceMap[seq]

  if !exists {
    return false
  }

  return inst.decided
}

func (px *Paxos) Proposer(seq int, v interface{}) {
  for !px.dead && !px.isDecided(seq) {
    //====================== Propose Phase ======================
    DPrintf("%v: seq: %v Propose Phase start\n", px.me, seq)

    proposeReplyChan := make(chan ProposeReply, len(px.peers) * 2)
    acceptReplyChan := make(chan AcceptReply, len(px.peers) * 2)

    N := time.Now().UnixNano()
    DPrintf("%v: N: %v, seq: %v, v: %v\n", px.me, N, seq, v)

    for i := 0; i < len(px.peers); i++ {
      if i == px.me {
        proposeArgs := &ProposeArgs{}
        var proposeReply ProposeReply

        proposeArgs.Seq = seq
        proposeArgs.N = N
        proposeArgs.MyDone = px.getPeersDone(px.me)
        proposeArgs.Me = px.me

        px.ProposeNHandler(proposeArgs, &proposeReply)
        proposeReplyChan <- proposeReply
      } else {
        go px.sendPropose(seq, N, i, proposeReplyChan)
      }
    }
    DPrintf("%v: Propose to all Done\n", px.me)

    okCount := 0
    notokCount := 0
    var replyMaxNa int64 = 0
    var replyMaxVa interface{} = nil

    majority := len(px.peers) / 2 + 1

    for {
      select {
      case reply := <- proposeReplyChan:
        if reply.Message == PROPOSE_OK {
          okCount++
          DPrintf("okCount:%v, notokCount:%v, majority:%v\n", okCount, notokCount, majority)
          if reply.Na > replyMaxNa {
            replyMaxNa = reply.Na
            replyMaxVa = reply.Va
          }
          if okCount >= majority {
            goto Propose_Get_Result
          }
        } else if reply.Message == DECIDED {
          v = reply.Va
          DPrintf("%v: seq: %v has been decided on %v\n", px.me, seq, v)
          goto Decided_Phase
        } else {
          notokCount++
          DPrintf("okCount:%v, notokCount:%v, majority:%v\n", okCount, notokCount, majority)
          if notokCount >= majority {
            goto Propose_Get_Result
          }
        }
      default:
      }    
    }

Propose_Get_Result:
    if okCount >= majority {
      DPrintf("%v: seq: %v, majority PROPOSE_OK\n", px.me, seq)
    } else {
      DPrintf("%v: seq: %v, majority PROPOSE_REJECT\n", px.me, seq)
      DPrintf("sleeping.............\n")
      time.Sleep(time.Duration(rand.Int63() % 200) * time.Millisecond)
      continue
    }

    //====================== Accept Phase ======================
    DPrintf("%v: seq: %v Accept Phase start\n", px.me, seq)

    if replyMaxVa != nil {
      v = replyMaxVa
    }

    for i := 0; i < len(px.peers); i++ {
      if i == px.me {
        acceptArgs := &AcceptArgs{}
        var acceptReply AcceptReply

        acceptArgs.Seq = seq
        acceptArgs.N = N
        acceptArgs.Val = v
        acceptArgs.MyDone = px.getPeersDone(px.me)
        acceptArgs.Me = px.me

        px.AcceptNVHandler(acceptArgs, &acceptReply)
        acceptReplyChan <- acceptReply
      } else {
        go px.sendAccept(seq, N, v, i, acceptReplyChan)
      }
    }

    okCount = 0
    notokCount = 0

    for {
      select {
      case reply := <- acceptReplyChan:
        if reply.Message == ACCEPT_OK {
          okCount++
          if okCount >= majority {
            goto Accept_Get_Result
          }
        } else if reply.Message == DECIDED {
          v = reply.Va
          DPrintf("%v: seq: %v has been decided on %v\n", px.me, seq, v)
          goto Decided_Phase
        } else {
          notokCount++
          if notokCount >= majority {
            goto Accept_Get_Result
          }
        }
      default:
      }
    }

Accept_Get_Result:
    if okCount >= majority {
      DPrintf("%v: seq: %v, majority ACCEPT_OK\n", px.me, seq)
    } else {
      DPrintf("%v: seq: %v, majority ACCEPT_REJECT\n", px.me, seq)
      DPrintf("sleeping.............\n")
      time.Sleep(time.Duration(rand.Int63() % 200) * time.Millisecond)
      continue
    }

    //====================== Decide Phase ======================
Decided_Phase:
    DPrintf("%v: seq: %v Decide Phase start\n", px.me, seq)
    if !px.isDecided(seq) {
      for i := 0; i < len(px.peers); i++ {
        if i == px.me {
          decideArgs := &DecideArgs{}
          var decideReply DecideReply

          decideArgs.Seq = seq
          decideArgs.Val = v
          decideArgs.MyDone = px.getPeersDone(px.me)
          decideArgs.Me = px.me

          px.DecideVHandler(decideArgs, &decideReply)
        } else {
          go px.sendDecide(seq, v, i)
        }
      }
    }

  }

}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  go px.Proposer(seq, v)
  DPrintf("finish start\n")
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  for k, _ := range px.instanceMap {
    if k <= seq {
      delete(px.instanceMap, k)
    }
  }

  px.setPeersDone(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  maxSeq := -1;

  for seq, _ := range px.instanceMap {
    if maxSeq < seq {
      maxSeq = seq
    }
  }

  return maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  minDone := math.MaxInt32
  for i := 0; i < len(px.peersDone); i++ {
    if minDone > px.peersDone[i] {
      minDone = px.peersDone[i]
    }
  }
  return minDone + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  inst, exists := px.instanceMap[seq]

  if !exists || !inst.decided {
    return false, nil
  }

  return true, inst.va 
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  // Your initialization code here.
  px.instanceMap = make(map[int]*Instance)
  px.peersDone = make([]int, len(peers))
  for i := 0; i < len(peers); i++ {
    px.peersDone[i] = -1
  }

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // Propose to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
