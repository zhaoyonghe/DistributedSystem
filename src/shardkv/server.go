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
  OrderID int64
  UID int64
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
  curConfig shardmaster.Config
  kvMap map[string]string
  orderidMap map[int64]int64
  replyMap map[int64]interface{}
}

func isEqualOp(op1 Op, op2 Op) bool {
  a := op1.OpType == op2.OpType
  b := op1.ClientID == op2.ClientID
  c := op1.UID == op2.UID
  d := op1.Config.Num == op2.Config.Num
  return a && b && c && d
}

func (kv *ShardKV) executeOp(op Op) interface{} {
  if op.OpType == GET {
    var reply GetReply
    value, ex := kv.kvMap[op.Key]
    if ex {
      reply.Err = OK
      reply.Value = value
    } else {
      reply.Err = ErrNoKey
      reply.Value = ""
    }
    kv.orderidMap[op.ClientID] = op.OrderID
    kv.replyMap[op.ClientID] = reply
    //DPrintf("GET\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v; uidMap:%v\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap, kv.uidMap)
    kv.px.Done(op.Seq)
    return reply
  }

  if op.OpType == PUT {
    var reply PutReply
    reply.Err = OK
    kv.kvMap[op.Key] = op.Value
    kv.orderidMap[op.ClientID] = op.OrderID
    kv.replyMap[op.ClientID] = reply
    //DPrintf("PUT\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
    kv.px.Done(op.Seq)
    return reply
  }

  if op.OpType == PUTHASH {
    var reply PutReply
    value, ex := kv.kvMap[op.Key]
    if ex {
      reply.Err = OK
      reply.PreviousValue = value
      kv.kvMap[op.Key] = strconv.Itoa(int(hash(value + op.Value)))
    } else {
      reply.Err = OK
      reply.PreviousValue = ""
      kv.kvMap[op.Key] = strconv.Itoa(int(hash(op.Value)))
    }
    kv.orderidMap[op.ClientID] = op.OrderID
    kv.replyMap[op.ClientID] = reply 
    kv.px.Done(op.Seq)
    //DPrintf("PUTHASH\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
    return reply      
  }

  if op.OpType == RECONFIGURE {
    for i := 0; i < shardmaster.NShards; i++ {
      if op.Config.Shards[i] == kv.gid && kv.curConfig.Shards[i] != kv.gid {
        args := &GrabShardsArgs{}
        args.Num = kv.curConfig.Num
        args.Shard = i
        var reply GrabShardsReply

        for _, srv := range kv.curConfig.Groups[kv.curConfig.Shards[i]] {
          ok := call(srv, "ShardKV.GrabShards", args, &reply)

          if ok && reply.Err == OK {
            for key, value := range reply.TransferKVMap {
              kv.kvMap[key] = value
            }
            
            for key, _ := range reply.TransferOrderIDMap {
              orderid, exists := kv.orderidMap[key]
              if !exists || orderid < reply.TransferOrderIDMap[key] {
                kv.orderidMap[key] = reply.TransferOrderIDMap[key]
                kv.replyMap[key] = reply.TransferReplyMap[key]
              }
              
            }
            break
          }

          if ok && reply.Err == ErrCannotGetYet {
            return "fail"
          }
        }
        DPrintf("%v, %v", op.Config.Shards, kv.curConfig.Shards)
        DPrintf("gid:%v me:%v grab from group %v, now:%v\n\n", kv.gid, kv.me, kv.curConfig.Shards[i], kv.kvMap)
      }
    }
    kv.curConfig = *copyConfig(op.Config)
    kv.px.Done(op.Seq)
    //DPrintf("RECONFIGURE\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
    return "success" 
  }

  return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  var op Op
  op.OpType = GET
  op.UID = nrand()
  op.ClientID = args.ClientID
  op.OrderID = args.OrderID
  op.Key = args.Key

  for {
    if kv.gid != kv.curConfig.Shards[key2shard(op.Key)] {
      reply.Err = ErrWrongGroup
      return nil
    }

    orderid, exists := kv.orderidMap[op.ClientID]
    if exists && orderid >= op.OrderID {
      tempReply := kv.replyMap[op.ClientID].(GetReply)
      reply.Err = tempReply.Err
      reply.Value = tempReply.Value
      return nil
    }

    kv.maxSeq += 1

    op.Seq = kv.maxSeq

    kv.px.Start(kv.maxSeq, op)
    result := kv.wait(kv.maxSeq)

    if isEqualOp(op, result) {
      // This op has been agreed by all replicas in this group
      tempReply := kv.executeOp(result).(GetReply)
      reply.Err = tempReply.Err
      reply.Value = tempReply.Value
      return nil
    } else {
      kv.executeOp(result)
    }
  }
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  var op Op
  if args.DoHash {
    op.OpType = PUTHASH
  } else {
    op.OpType = PUT
  }
  op.UID = nrand()
  op.ClientID = args.ClientID
  op.Key = args.Key
  op.Value = args.Value

  for {
    if kv.gid != kv.curConfig.Shards[key2shard(op.Key)] {
      reply.Err = ErrWrongGroup
      return nil
    }

    orderid, exists := kv.orderidMap[op.ClientID]
    if exists && orderid >= op.OrderID {
      tempReply := kv.replyMap[op.ClientID].(PutReply)
      reply.Err = tempReply.Err
      reply.PreviousValue = tempReply.PreviousValue
      return nil
    }

    kv.maxSeq += 1

    op.Seq = kv.maxSeq

    kv.px.Start(kv.maxSeq, op)
    result := kv.wait(kv.maxSeq)

    if isEqualOp(op, result) {
      // This op has been agreed by all replicas in this group
      tempReply := kv.executeOp(result).(PutReply)
      reply.Err = tempReply.Err
      reply.PreviousValue = tempReply.PreviousValue
      return nil
    } else {
      kv.executeOp(result)
    }
  }
}

func (kv *ShardKV) GrabShards(args *GrabShardsArgs, reply *GrabShardsReply) error {
  if kv.curConfig.Num < args.Num {
    DPrintf("ErrCannotGetYet\n\n")
    reply.Err = ErrCannotGetYet
    return nil
  }

  kv.mu.Lock()
  defer kv.mu.Unlock()

  reply.TransferKVMap = make(map[string]string)
  reply.TransferOrderIDMap = make(map[int64]int64)
  reply.TransferReplyMap = make(map[int64]interface{})

  for key, value := range kv.kvMap {
    DPrintf("%v\n", key)
    if key2shard(key) == args.Shard {
      DPrintf("bbbbbbbbbbb\n\n\n")
      reply.TransferKVMap[key] = value
    }
  }

  for key, _ := range kv.orderidMap {
      reply.TransferOrderIDMap[key] = kv.orderidMap[key]
      reply.TransferReplyMap[key] = kv.replyMap[key]
  }

  reply.Err = OK

  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  latestConfig := kv.sm.Query(-1)
  from := kv.curConfig.Num
  to := latestConfig.Num
  for num := from + 1; num <= to; num++ {
    DPrintf("gid:%v, me:%v, query config:%v\n\n", kv.gid, kv.me, num)
    newConfig := kv.sm.Query(num)
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
        // This op has been agreed by all replicas in this group
        str := kv.executeOp(result).(string)
        if str == "success" {
          break
        } else {
          // str == "fail"
          return
        }
      } else {
        kv.executeOp(result)
      }
    }
  }
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
  kv.curConfig.Groups = make(map[int64][]string)
  kv.kvMap = make(map[string]string)
  kv.orderidMap = make(map[int64]int64)
  kv.replyMap = make(map[int64]interface{})

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