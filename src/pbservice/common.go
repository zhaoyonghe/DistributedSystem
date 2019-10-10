package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
	UID int64

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  UID int64
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.
type CheckNewArgs struct {

}

type CheckNewReply struct {
	New bool
}

type TransferMapArgs struct {
	StMap map[string] string
}

type TransferMapReply struct {
	Received bool
}



func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

