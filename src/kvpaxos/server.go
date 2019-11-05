package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  OpType string
  Key string
  Value string
  DoHash bool
  UID int64
  ClientID int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  // Your definitions here.
  kvMap map[string]string
  uidMap map[int64]int64
  replyMap map[int64]interface{}
  maxSeq int
}

func (kv *KVPaxos) wait(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, v := kv.px.Status(seq)
    if decided {
      return v.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

func (kv *KVPaxos) executeOp(op Op) interface{} {
  for {
    // if has been done before
    uid, exists := kv.uidMap[op.ClientID]
    if exists && uid == op.UID {
      return kv.replyMap[op.ClientID]
    }

    // has not been done before
    // try to catch up, let majority agree with and execute this operation
    kv.maxSeq++

    decided, v := kv.px.Status(kv.maxSeq)

    var result Op
    if decided {
      // lag behind, need to catch up
      result = v.(Op)
    } else {
      // do not lag behind, try to agree with this operation
      kv.px.Start(kv.maxSeq, op)
      result = kv.wait(kv.maxSeq)
    }

    // execute result operation
    value, exists := kv.kvMap[result.Key]

    if result.OpType == Put {
      var reply PutReply
      if result.DoHash {
        kv.kvMap[result.Key] = strconv.Itoa(int(hash(value + result.Value)))
        reply.Err = OK
        if exists {
          reply.PreviousValue = value
        } else {
          reply.PreviousValue = ""
        }
      } else {
        kv.kvMap[result.Key] = result.Value
        reply.Err = OK
        reply.PreviousValue = ""
      }
      kv.uidMap[result.ClientID] = result.UID
      kv.replyMap[result.ClientID] = reply
    } else {
      // op.OpType == Get
      var reply GetReply
      if exists {
        reply.Err = OK
        reply.Value = value
      } else {
        reply.Err = ErrNoKey
        reply.Value = ""
      } 
      kv.uidMap[result.ClientID] = result.UID
      kv.replyMap[result.ClientID] = reply
    }

    kv.px.Done(kv.maxSeq)

    if result.UID == op.UID {
      // majority has agreed with this operation
      // successfully catch up and execute this operation
      // same as return kv.replyMap[result.ClientID]
      return kv.replyMap[op.ClientID]
    }
  }

}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  var op Op

  op.OpType = Get
  op.Key = args.Key
  op.UID = args.UID
  op.ClientID = args.ClientID

  reply1 := kv.executeOp(op).(GetReply)

  reply.Err = reply1.Err
  reply.Value = reply1.Value

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  var op Op

  op.OpType = Put
  op.Key = args.Key
  op.UID = args.UID
  op.ClientID = args.ClientID
  op.Value = args.Value
  op.DoHash = args.DoHash

  reply1 := kv.executeOp(op).(PutReply)

  reply.Err = reply1.Err
  reply.PreviousValue = reply1.PreviousValue

  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me
  // Your initialization code here.
  kv.kvMap = make(map[string] string)
  kv.uidMap = make(map[int64] int64)
  kv.replyMap = make(map[int64] interface{})
  kv.maxSeq = 0


  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

