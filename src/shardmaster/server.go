package shardmaster

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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

const (
  JOIN = "JOIN"
  LEAVE = "LEAVE"
  MOVE = "MOVE"
  QUERY = "QUERY"
)


type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  maxSeq int
}

func (sm *ShardMaster) wait(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, v := sm.px.Status(seq)
    if decided {
      return v.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}

type Op struct {
  // Your data here.
  Seq int
  Num int
  OpType string
  Shards [NShards]int64
  Groups map[int64][]string
}

func checkSameOp(op1 Op, op2 Op) bool {
  if op1.Seq != op2.Seq {
    return false
  }

  if op1.Num != op2.Num {
    return false
  }

  if op1.OpType != op2.OpType {
    return false
  }

  for i := 0; i < NShards; i++ {
    if op1.Shards[i] != op2.Shards[i] {
      return false
    }
  }

  if len(op1.Groups) != len(op2.Groups) {
    return false
  }

  for gid, _ := range op1.Groups {
    _, exists := op2.Groups[gid]
    if !exists {
      return false
    }
  }

  return true
}

func copyConfig(config Config) (*Config){
  var con Config
  con.Num = config.Num
  con.Shards = config.Shards
  con.Groups = make(map[int64][]string)
  for k, v := range config.Groups {
    con.Groups[k] = v
  }
  return &con
}

func buildConfig(op Op) (*Config){
  var con Config
  con.Num = op.Num
  con.Shards = op.Shards
  con.Groups = make(map[int64][]string)
  for k, v := range op.Groups {
    con.Groups[k] = v
  }
  return &con
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  for {
    sm.maxSeq += 1

    decided, v := sm.px.Status(sm.maxSeq) 

    var op Op
    if decided {
      DPrintf("JOIN&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n")
      // lag behind, need to catch up
      op = v.(Op)

      // build new config to catch up
      if op.OpType != QUERY {
        var config = *buildConfig(op)
        sm.configs = append(sm.configs, config)
      }

      sm.px.Done(sm.maxSeq)
      continue
    } else {
      // do not lag behind, try to agree with this operation
      var nextNum = len(sm.configs)
      var curConfig = sm.configs[len(sm.configs) - 1]

      op.Seq = sm.maxSeq
      op.Num = nextNum
      op.OpType = JOIN

      _, exists := curConfig.Groups[args.GID]
      if exists {
        // join the group that current config already has
        op.Shards = curConfig.Shards
      } else {
        // join a new group
        groupsNum := len(curConfig.Groups)
        if groupsNum == 0 {
          // first join
          for i := 0; i < NShards; i++ {
            op.Shards[i] = args.GID
          }
        } else if groupsNum == NShards {
          // no available shards
          op.Shards = curConfig.Shards
        } else {
          // groupsNum > 0
          // group id -> [shard1, shard2, ...]
          var groupsShards = make(map[int64][]int)
          for i := 0; i < NShards; i++ {
            groupsShards[curConfig.Shards[i]] = append(groupsShards[curConfig.Shards[i]], i)
          }
          DPrintf("%v\n", groupsShards)

          minShards := NShards / (groupsNum + 1)
          maxShards := minShards
          if NShards % (groupsNum + 1) != 0 {
            maxShards += 1
          }

          // rebalance
          var newGroupShards = []int{}
          for _, shards := range groupsShards {
            newGroupShards = append(newGroupShards, shards[minShards:len(shards)]...)
            shards = shards[:minShards]
            if len(newGroupShards) == maxShards {
              break
            }
            if len(newGroupShards) > maxShards {
              shards = append(shards, newGroupShards[len(newGroupShards) - 1])
              newGroupShards = newGroupShards[:len(newGroupShards) - 1]
              break
            }
          }

          op.Shards = curConfig.Shards
          for i := 0; i < len(newGroupShards); i++ {
            op.Shards[newGroupShards[i]] = args.GID
          }
        }
      }
      
      op.Groups = make(map[int64][]string)
      for gid, servers := range curConfig.Groups {
        op.Groups[gid] = servers
      }
      op.Groups[args.GID] = args.Servers
    }

    sm.px.Start(sm.maxSeq, op)
    result := sm.wait(sm.maxSeq)

    sm.configs = append(sm.configs, *buildConfig(result))
    sm.px.Done(sm.maxSeq)

    if checkSameOp(op, result) {
      return nil
    }
  }
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  for {
    sm.maxSeq += 1

    decided, v := sm.px.Status(sm.maxSeq) 

    var op Op
    if decided {
      DPrintf("LEAVE&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n")
      // lag behind, need to catch up
      op = v.(Op)

      // build new config to catch up
      if op.OpType != QUERY {
        var config = *buildConfig(op)
        sm.configs = append(sm.configs, config)      
      }

      sm.px.Done(sm.maxSeq)
      continue
    } else {
      // do not lag behind, try to agree with this operation
      var nextNum = len(sm.configs)
      var curConfig = sm.configs[len(sm.configs) - 1]

      op.Seq = sm.maxSeq
      op.Num = nextNum
      op.OpType = LEAVE

      _, exists := curConfig.Groups[args.GID]
      if !exists {
        // leave a group that current config does not have
        op.Shards = curConfig.Shards
      } else {
        // leave a group that current config has
        groupsNum := len(curConfig.Groups)
        if groupsNum == 1 {
          // last leave
          for i := 0; i < NShards; i++ {
            op.Shards[i] = 0
          }
        } else {
          // groupsNum > 1
          // group id -> [shard1, shard2, ...]
          var groupsShards = make(map[int64][]int)
          var leaveGroupShards = []int{}
          for gid, _ := range curConfig.Groups {
            if gid != args.GID {
              groupsShards[gid] = []int{}
            }
          }
          for i := 0; i < NShards; i++ {
            if curConfig.Shards[i] == args.GID {
              leaveGroupShards = append(leaveGroupShards, i)
            } else {
              groupsShards[curConfig.Shards[i]] = append(groupsShards[curConfig.Shards[i]], i)
            }
          }
          //DPrintf("%v\n", leaveGroupShards)
          //DPrintf("%v\n", groupsShards)

          minShards := NShards / (groupsNum - 1)
          maxShards := minShards
          if NShards % (groupsNum - 1) != 0 {
            maxShards += 1
          }

          // rebalance
          for gid, shards := range groupsShards {
            temp := len(leaveGroupShards) - (maxShards - len(shards))
            //DPrintf("asd%v\nasd%v\n", leaveGroupShards, groupsShards)
            //DPrintf("temp: %v; len(l): %v, maxShards:%v, len(s): %v, groupsNum: %v", 
            //temp, len(leaveGroupShards), maxShards, len(shards), groupsNum)
            if temp > 0 {
              groupsShards[gid] = append(shards, leaveGroupShards[temp:len(leaveGroupShards)]...)
              //DPrintf("kkkk%v\n", shards)
            } else {
              groupsShards[gid] = append(shards, leaveGroupShards[0:len(leaveGroupShards)]...)
              //DPrintf("kkkk%v\n", shards)
              break
            }
          }

          for gid, shards := range groupsShards {
            //DPrintf("PPPP\n")
            for i := 0; i < len(shards); i++ {
              //DPrintf("di %v, %v\n", gid, i)
              op.Shards[shards[i]] = gid
            }
            //DPrintf("%v\n", op.Shards)
          }
        }
      }

      op.Groups = make(map[int64][]string)
      for gid, servers := range curConfig.Groups {
        if gid != args.GID {
          op.Groups[gid] = servers
        }
      }
    }

    sm.px.Start(sm.maxSeq, op)
    result := sm.wait(sm.maxSeq)

    sm.configs = append(sm.configs, *buildConfig(result))
    sm.px.Done(sm.maxSeq)

    if checkSameOp(op, result) {
      return nil
    }
  }
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  for {
    sm.maxSeq += 1
    
    decided, v := sm.px.Status(sm.maxSeq) 

    var op Op
    if decided {
      DPrintf("MOVE&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&\n")
      // lag behind, need to catch up
      op = v.(Op)

      // build new config to catch up
      for op.OpType != QUERY {
        var config = *buildConfig(op)
        sm.configs = append(sm.configs, config)        
      }

      sm.px.Done(sm.maxSeq)
      continue
    } else {
      // do not lag behind, try to agree with this operation
      var nextNum = len(sm.configs)
      var curConfig = sm.configs[len(sm.configs) - 1]

      op.Seq = sm.maxSeq
      op.Num = nextNum
      op.OpType = MOVE
      op.Shards = curConfig.Shards

      _, exists := curConfig.Groups[args.GID]
      if exists {
        op.Shards[args.Shard] = args.GID
      }

      op.Groups = make(map[int64][]string)
      for gid, servers := range curConfig.Groups {
          op.Groups[gid] = servers
      }
    }

    sm.px.Start(sm.maxSeq, op)
    result := sm.wait(sm.maxSeq)

    sm.configs = append(sm.configs, *buildConfig(result))
    sm.px.Done(sm.maxSeq)

    if checkSameOp(op, result) {
      return nil
    }
  }
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()

  for {
    sm.maxSeq += 1

    var op Op
    op.Seq = sm.maxSeq
    op.OpType = QUERY

    sm.px.Start(sm.maxSeq, op)
    result := sm.wait(sm.maxSeq)

    if checkSameOp(op, result) {
      if args.Num == -1 || args.Num >= len(sm.configs) {
        reply.Config = *copyConfig(sm.configs[len(sm.configs) - 1])
      } else {
        if args.Num < len(sm.configs) && args.Num >= 0 {
          reply.Config = *copyConfig(sm.configs[args.Num])
        }
      }
      sm.px.Done(sm.maxSeq)
      return nil
    } else {
      if result.OpType != QUERY {
        sm.configs = append(sm.configs, *buildConfig(result))
      }
      sm.px.Done(sm.maxSeq)
    }
  }
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
