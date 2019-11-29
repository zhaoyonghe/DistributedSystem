package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 1

const (
  GET = "GET"
  PUT = "PUT"
  PUTHASH = "PUTHASH"
  RECONFIGURE = "RECONFIGURE"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}

func (kv *ShardKV) wait(seq int) Op {
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

func copyConfig(config shardmaster.Config) (*shardmaster.Config){
  var con shardmaster.Config
  con.Num = config.Num
  con.Shards = config.Shards
  con.Groups = make(map[int64][]string)
  for k, v := range config.Groups {
    con.Groups[k] = v
  }
  return &con
}

type Op struct {
  // Your definitions here.
  Seq int
  OpType string
  ClientID int64
  UID int64
  Shard int
  Key string
  Value string
  Config shardmaster.Config
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  maxSeq int
  curSeq int
  curConfig shardmaster.Config
  kvMap map[string]string
  uidMap map[int64]interface{}
}

func isEqualOp(op1 Op, op2 Op) bool {
  a := op1.OpType == op2.OpType
  b := op1.ClientID == op2.ClientID
  c := op1.UID == op2.UID
  d := op1.Config.Num == op2.Config.Num
  return a && b && c && d
}

func (kv *ShardKV) syncOps() {
  kv.curSeq += 1
  for {
    decided, v := kv.px.Status(kv.curSeq)
    if !decided {
      DPrintf("=================================================\n")
      DPrintf("\n\n\n\n\n\nERROR! NOT DECIDED!\n\n\n\n\n\n")
      DPrintf("=================================================\n")
    }
    var op Op
    op = v.(Op)
    if op.OpType == GET {
      var reply GetReply
      if kv.curConfig.Shards[key2shard(op.Key)] == kv.gid {
        value, exists := kv.kvMap[op.Key]
        if exists {
          reply.Err = OK
          reply.Value = value
        } else {
          reply.Err = ErrNoKey
          reply.Value = ""
        }
      } else {
        reply.Err = ErrWrongGroup
        reply.Value = ""
      }
      kv.uidMap[op.UID] = reply
    } else if op.OpType == PUT {
      var reply PutReply
      if kv.curConfig.Shards[key2shard(op.Key)] == kv.gid {
        reply.Err = OK
        reply.PreviousValue = ""
        kv.kvMap[op.Key] = op.Value
      } else {
        reply.Err = ErrWrongGroup
        reply.PreviousValue = ""      
      }    
      kv.uidMap[op.UID] = reply
    } else if op.OpType == PUTHASH {
      var reply PutReply
      if kv.curConfig.Shards[key2shard(op.Key)] == kv.gid {
        value, exists := kv.kvMap[op.Key]
        if exists {
          reply.Err = OK
          reply.PreviousValue = value
          kv.kvMap[op.Key] = strconv.Itoa(int(hash(value + op.Value)))
        } else {
          reply.Err = OK
          reply.PreviousValue = ""
          kv.kvMap[op.Key] = strconv.Itoa(int(hash(op.Value)))
        }
      } else {
        reply.Err = ErrWrongGroup
        reply.PreviousValue = ""
      }
      kv.uidMap[op.UID] = reply     
    } else if op.OpType == RECONFIGURE {
      // check if needs to hand off the shards
      for i := 0; i < shardmaster.NShards; i++ {
        if kv.curConfig.Shards[i] == kv.gid && op.Config.Shards[i] != kv.gid {
          DPrintf("gid:%v, me:%v, DIFFERENT:%v\n\n", kv.gid, kv.me, i)
          args := &ReceiveArgs{}
          args.TransferMap = make(map[string]string)

          var reply ReceiveReply

          for key, value := range kv.kvMap {
            if key2shard(key) == i {
              args.TransferMap[key] = value
            }
          }

          if len(args.TransferMap) != 0 {
            //args.UID = int64(op.Config.Num)
            DPrintf("gid:%v, me:%v, yDIFFERENT:%v\n\n", kv.gid, kv.me, i)
            DPrintf("gid:%v, me:%v, want to transfer:%v\n\n", kv.gid, kv.me, args.TransferMap)
            DPrintf("from gid %v to gid %v\n\n", kv.gid, op.Config.Shards[i])
            args.UID = nrand()
            replicaAddrs := op.Config.Groups[op.Config.Shards[i]]
            for k := 0; k < len(replicaAddrs); k++ {
              for j := 0; j < 15; j++ {
                ok := call(replicaAddrs[k], "ShardKV.Receive", args, &reply)
                if ok && reply.Err == OK {
                  break
                }
              }
            }
          }
        }
      }
      kv.curConfig = op.Config
    } else {
      DPrintf("=================================================\n")
      DPrintf("\n\n\n\n\n\nERROR! EMPTY OP!\n\n\n\n\n\n")
      DPrintf("=================================================\n")
    }

    kv.px.Done(kv.curSeq)
    if kv.curSeq == kv.maxSeq {
      return
    } else {
      kv.curSeq += 1
    }
  }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.smallTick()

  var op Op
  op.OpType = GET
  op.UID = args.UID
  op.ClientID = args.ClientID
  op.Shard = args.Shard
  op.Key = args.Key

  for {
    kv.maxSeq += 1

    op.Seq = kv.maxSeq

    kv.px.Start(kv.maxSeq, op)
    result := kv.wait(kv.maxSeq)

    if isEqualOp(op, result) {
      kv.syncOps()
      DPrintf("GET\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
      tempReply := kv.uidMap[op.UID].(GetReply)
      reply.Err = tempReply.Err
      reply.Value = tempReply.Value
      return nil
    }
  }
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.smallTick()

  var op Op
  if args.DoHash {
    op.OpType = PUTHASH
  } else {
    op.OpType = PUT
  }
  op.UID = args.UID
  op.ClientID = args.ClientID
  op.Shard = args.Shard
  op.Key = args.Key
  op.Value = args.Value

  for {
    kv.maxSeq += 1

    op.Seq = kv.maxSeq

    kv.px.Start(kv.maxSeq, op)
    result := kv.wait(kv.maxSeq)

    if isEqualOp(op, result) {
      kv.syncOps()
      DPrintf("PUT/PUTHASH\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
      tempReply := kv.uidMap[op.UID].(PutReply)
      reply.Err = tempReply.Err
      reply.PreviousValue = tempReply.PreviousValue
      return nil
    }
  }
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.smallTick()
}

func (kv *ShardKV) smallTick() {
  newConfig := kv.sm.Query(-1)
  if newConfig.Num != kv.curConfig.Num {
    // a new confiuration
    var op Op
    op.OpType = RECONFIGURE
    op.ClientID = -1
    op.UID = -1
    op.Config = *copyConfig(newConfig)

    for {
      kv.maxSeq += 1

      op.Seq = kv.maxSeq

      kv.px.Start(kv.maxSeq, op)
      result := kv.wait(kv.maxSeq)

      if isEqualOp(op, result) {
        kv.syncOps()
        return
      }
    }
  }
}


func (kv *ShardKV) Receive(args *ReceiveArgs, reply *ReceiveReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  DPrintf("gid:%v RECEIVED IN", kv.gid)

  var lastReply ReceiveReply

  mapReply, exists := kv.uidMap[args.UID]
  if exists {
    lastReply = mapReply.(ReceiveReply)
    reply.Err = lastReply.Err
    DPrintf("HAS BEEN RECEIVED TransferMap:%v\n\n", args.TransferMap)
    return nil
  }

  // have not received yet
  for key, value := range args.TransferMap {
    kv.kvMap[key] = value
  }

  lastReply.Err = OK
  reply.Err = OK

  kv.uidMap[args.UID] = lastReply
  DPrintf("RECEIVED TransferMap:%v\n\n", args.TransferMap)

  return nil
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().

  kv.maxSeq = 0
  kv.curSeq = 0
  kv.curConfig.Groups = make(map[int64][]string)
  kv.kvMap = make(map[string]string)
  kv.uidMap = make(map[int64]interface{})

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}