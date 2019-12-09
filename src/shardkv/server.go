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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET = "GET"
	PUT = "PUT"
	PUTHASH = "PUTHASH"
	RECONFIGURE = "RECONFIGURE"
)

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
	DPrintf("=============================================\n")
	DPrintf("=============================================\n")
	DPrintf("=============================================\n")
	DPrintf("gid:%v, me:%v, waitfor:%v time:%v\n", kv.gid, kv.me, seq, to)
	DPrintf("=============================================\n")
	DPrintf("=============================================\n")
	DPrintf("=============================================\n")
	DPrintf("=============================================\n")
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
	OpType string
	UID int64
	Key string
	Value string
	Config shardmaster.Config
	TransferKVMap map[string]string
	TransferUIDMap map[int64]interface{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	maxSeq int
	curConfig shardmaster.Config
	kvMap map[string]string
	uidMap map[int64]interface{}
}

func isEqualOp(op1 Op, op2 Op) bool {
  a := op1.OpType == op2.OpType
  //b := op1.ClientID == op2.ClientID
  b := true
  c := op1.UID == op2.UID
  d := op1.Config.Num == op2.Config.Num
  return a && b && c && d
}

func (kv *ShardKV) executeOp(op Op) {
	if op.OpType == GET {
		var reply GetReply
		val, ex := kv.kvMap[op.Key]
		if ex {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.uidMap[op.UID] = reply
		DPrintf("GET\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v; uidMap:%v\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap, kv.uidMap)
		return
	}

	if op.OpType == PUT {
		var reply PutReply
		reply.Err = OK
		kv.uidMap[op.UID] = reply
		kv.kvMap[op.Key] = op.Value
		DPrintf("PUT\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
		return
	}

	if op.OpType == PUTHASH {
		var reply PutReply
		reply.Err = OK
		val, exists := kv.kvMap[op.Key]
		if exists {
			reply.PreviousValue = val
			kv.uidMap[op.UID] = reply
			kv.kvMap[op.Key] = strconv.Itoa(int(hash(val + op.Value)))
		} else {
			reply.PreviousValue = ""
			kv.uidMap[op.UID] = reply
			kv.kvMap[op.Key] = strconv.Itoa(int(hash(op.Value)))
		}
		DPrintf("PUTHASH\n gid:%v;\n me:%v;\n curConfig:%v;\n op:%v;\n kvMap:%v;\n\n", kv.gid, kv.me, kv.curConfig, op, kv.kvMap)
		return
	}

	if op.OpType == RECONFIGURE {
		for key, value := range op.TransferKVMap {
			kv.kvMap[key] = value
		}
		for key, value := range op.TransferUIDMap {
			kv.uidMap[key] = value
		}
		kv.curConfig = *copyConfig(op.Config)
		DPrintf("RECONFIGURE gid:%v me:%v cur:%v, op:%v\n\n", kv.gid, kv.me, kv.curConfig.Num, op.Config.Num)
		return
	}   
}

func (kv *ShardKV) syncOps(toSeq int) {
	for kv.maxSeq + 1 < toSeq {
		kv.maxSeq += 1
		to := 10 * time.Millisecond
		first := false
		for {
			decided, v := kv.px.Status(kv.maxSeq)
			if decided {
				kv.executeOp(v.(Op))
				break
			}
			if !first {
				var op Op
				kv.px.Start(kv.maxSeq, op)
				first = true
				//DPrintf("duck:%v", kv.maxSeq)
			}
			
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}

	kv.px.Done(kv.maxSeq)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var op Op
	op.OpType = GET
	op.UID = args.UID
	op.Key = args.Key

	shard := key2shard(op.Key)

	for {
		seq := kv.px.Max() + 1
		kv.syncOps(seq)
		if kv.curConfig.Shards[shard] != kv.gid {
			reply.Err = ErrWrongGroup
			return nil
		}
		v, ex := kv.uidMap[op.UID]
		if ex {
			// already have
			vv := v.(GetReply)
			reply.Err, reply.Value = vv.Err, vv.Value
			return nil
		}

		kv.px.Start(seq, op)
		to := 10 * time.Millisecond
		for {
			decided, _ := kv.px.Status(seq)
			if decided {
				kv.syncOps(kv.px.Max() + 1)
				if kv.curConfig.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
					return nil
				}

				v, ex := kv.uidMap[op.UID]
				if ex {
					// already have
					vv := v.(GetReply)
					reply.Err, reply.Value = vv.Err, vv.Value
					return nil
				}
				break
			}

			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}
	return nil
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
	op.UID = args.UID
	op.Key = args.Key
	op.Value = args.Value

	shard := key2shard(op.Key)

	for {
		seq := kv.px.Max() + 1
		kv.syncOps(seq)
		if kv.curConfig.Shards[shard] != kv.gid {
			reply.Err = ErrWrongGroup
			return nil
		}
		v, ex := kv.uidMap[op.UID]
		if ex {
			// already have
			vv := v.(PutReply)
			reply.Err, reply.PreviousValue = vv.Err, vv.PreviousValue
			return nil
		}

		kv.px.Start(seq, op)
		to := 10 * time.Millisecond
		for {
			decided, _ := kv.px.Status(seq)
			if decided {
				kv.syncOps(kv.px.Max() + 1)
				if kv.curConfig.Shards[shard] != kv.gid {
					reply.Err = ErrWrongGroup
					return nil
				}

				v, ex := kv.uidMap[op.UID]
				if ex {
					// already have
					vv := v.(PutReply)
					reply.Err, reply.PreviousValue = vv.Err, vv.PreviousValue
					return nil
				}
				break
			}

			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
		}
	}

	return nil
}

func (kv *ShardKV) GrabShards(args *GrabShardsArgs, reply *GrabShardsReply) error {
	if kv.curConfig.Num < args.Num {
		reply.Err = ErrCannotGetYet
		DPrintf("I am %v, ErrCannotGetYet kv.curConfig.Num:%v; args.Num:%v\n\n", kv.gid, kv.curConfig.Num, args.Num)
		return nil
	}

	reply.TransferKVMap = make(map[string]string)
	reply.TransferUIDMap = make(map[int64]interface{})

	count := 0

	for key, value := range kv.kvMap {
		if key2shard(key) == args.Shard {
			DPrintf("bbbbbbbbbbb\n\n\n")
			reply.TransferKVMap[key] = value
			count += 1
		}
	}

	DPrintf("count:%v", count)

	for key, value := range kv.uidMap {
		reply.TransferUIDMap[key] = value
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

	seq := kv.px.Max() + 1
	kv.syncOps(seq)

	// once a time
	latestConfig := kv.sm.Query(kv.curConfig.Num + 1)
	if latestConfig.Num == kv.curConfig.Num {
		return
	}
	if latestConfig.Num < kv.curConfig.Num {
		DPrintf("sadfadfasdfafeww!!!!!!!!!!!!!!!11\n\n")
	}

	var op Op
	op.OpType = RECONFIGURE
	op.UID = -1
	op.Config = *copyConfig(latestConfig)
	op.TransferKVMap = make(map[string]string)
	op.TransferUIDMap = make(map[int64]interface{})

	for i := 0; i < shardmaster.NShards; i++ {
		if latestConfig.Shards[i] == kv.gid && kv.curConfig.Shards[i] != kv.gid && kv.curConfig.Shards[i] != 0 {
			DPrintf("i:%v, latestConfig.Shards[i]:%v, kv.curConfig.Shards[i]:%v, kv.gid:%v",
			 i, latestConfig.Shards[i], kv.curConfig.Shards[i], kv.gid)
			args := &GrabShardsArgs{}
			args.Num = latestConfig.Num
			args.Shard = i
			var reply GrabShardsReply

iterativeGrab:
			for {
				for _, srv := range kv.curConfig.Groups[kv.curConfig.Shards[i]] {
					ok := call(srv, "ShardKV.GrabShards", args, &reply)

					if ok && reply.Err == OK {
					for key, value := range reply.TransferKVMap {
						op.TransferKVMap[key] = value
					}
					
					for key, value := range reply.TransferUIDMap {
						op.TransferUIDMap[key] = value
					}
					break iterativeGrab
					}

					if ok && reply.Err == ErrCannotGetYet {
					//return "fail"
					}
				}
				DPrintf("*********************\n")
				DPrintf("*********************\n")
				DPrintf("*********************\n")
				DPrintf("kjhkjhkjhkjhj fail fail\n") 
				DPrintf("*********************\n")
				DPrintf("*********************\n")
				DPrintf("*********************\n")
			}

			DPrintf("%v, %v", latestConfig.Shards, kv.curConfig.Shards)
			DPrintf("gid:%v me:%v grab from group %v, now:%v\n\n", kv.gid, kv.me, kv.curConfig.Shards[i], kv.kvMap)
		}
	}

	for {
		seq := kv.px.Max() + 1
		kv.syncOps(seq)
		if kv.curConfig.Num >= latestConfig.Num {
			return
		}

		kv.px.Start(seq, op)
		to := 10 * time.Millisecond
		for {
			decided, _ := kv.px.Status(seq)
			if decided {
				kv.syncOps(kv.px.Max() + 1)
				if kv.curConfig.Num >= latestConfig.Num {
					return
				}
				break
			}

			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
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
	gob.Register(PutReply{})
	gob.Register(GetReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	kv.maxSeq = -1
	kv.curConfig.Groups = make(map[int64][]string)
	kv.kvMap = make(map[string]string)
	kv.uidMap = make(map[int64]interface{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
