package pbservice

import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const Debug = 1


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op    string
	Id    string 
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id  string
}

type GetReply struct {
	Err   Err
	Value string
}

type ForwardArgs struct {
	Key   string
	Value string
	Op    string
	Id    string
}

type ForwardReply struct {
	Value string
	Err   Err
}


// Your RPC definitions here.
type TransferArgs struct {
	Database map[string] string
	Received map[string] bool
}

type TransferReply struct {
	Err Err
}