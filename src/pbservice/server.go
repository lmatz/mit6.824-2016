package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"
// import "strconv"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currentView viewservice.View
	database    map[string] string
	received   map[string] bool
}

func (pb *PBServer) isPrimary() bool {
	return pb.currentView.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return pb.currentView.Backup == pb.me
}

func (pb *PBServer) hasPrimary() bool {
	return pb.currentView.Primary != ""
}

func (pb *PBServer) hasBackup() bool {
	return pb.currentView.Backup != ""
}

func (pb *PBServer) transferDatabaseToBackup() {
	if !pb.isPrimary() {

		DPrintf("Impossible: %s should be a primary.",pb.me)
		return 
	}

	if !pb.hasBackup() {

		DPrintf("Impossible: %s is a primary and it should have a backup.",pb.me)
		return 
	}

	if pb.isPrimary() && pb.hasBackup() {

		transferArgs := new(TransferArgs)
		transferArgs.Database = pb.database
		transferArgs.Received = pb.received
		
		transferReply := new(TransferReply)
		
		for {
			ok := call(pb.currentView.Backup,"PBServer.GetDatabaseFromPrimary",transferArgs,transferReply)
			if ok {
				return
			}
			time.Sleep(viewservice.PingInterval)
		}

	} 

}

func (pb *PBServer) GetDatabaseFromPrimary(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isBackup() {

		pb.database = make(map[string] string)
		for key, value := range args.Database {
			pb.database[key] = value
		}
		pb.received = make(map[string] bool)
		for key, value := range args.Received {
			pb.received[key] = value
		}
		reply.Err = OK

		return nil

	} else {
		DPrintf("Impossible: %s should be a backup.",pb.me)
		reply.Err = ErrWrongServer
		return errors.New(pb.me+" is not backup. Database transfer is invalid.")
	}
}

// bool indicates whether this operation is done by the backup
// the reason why the backup would fail
func (pb *PBServer) forwardOpToBackup(key string, value string, op string, id string) bool {

	if !pb.hasBackup() {
		return true
	}

	if pb.isPrimary() {

		forwardArgs := new(ForwardArgs)
		forwardArgs.Key = key
		forwardArgs.Value = value
		forwardArgs.Op = op
		forwardArgs.Id = id

		forwardReply := new(ForwardReply)

		ok := call(pb.currentView.Backup,"PBServer.ProcessForward",forwardArgs,forwardReply)
		return ok

	} else {

		DPrintf("Impossible: %s should be a primary.",pb.me)
		return false
	}
}

func (pb *PBServer) ProcessForward(args *ForwardArgs, reply *ForwardReply) error {
	key := args.Key
	value := args.Value
	op := args.Op
	id := args.Id

	if pb.isBackup() {

		if id != "" && pb.received[id] {
			return nil
		}

		// if pb is backup, then it is ok
		// just process the op and return ok to primary

		val, ok := pb.database[key]

		switch op {
		case "Get":
			if  ok {
				reply.Value = val
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
		case "Put":
			pb.database[key] = value
			reply.Err = OK
		case "Append":
			pb.database[key] = val + value
			reply.Err = OK
		}

		if id != "" {
			pb.received[id] = true
		}

		return nil

	} else {
		// if pb is not backup, then it returns with error
		reply.Err = ErrWrongServer
		return errors.New(pb.me+" is not primary. Cannot process forward.")
	}
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	key := args.Key
	id := args.Id

	if !pb.isPrimary() {
		// if the current server is not primary,
		// then tell clients that wrong server error

		reply.Err = ErrWrongServer
		return errors.New(pb.me+" is not primary server. The 'Get' operation is invalid.")
	} 

	// if the current server is primary,
	// then it should forward this 'Get' to backup
	if pb.forwardOpToBackup(key,"","Get",id) {
		if val, ok := pb.database[key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey 
			reply.Value = ""
		}
	} else {
		DPrintf("Get"+" forward to backup fail")
		reply.Err = ErrWrongServer
		return errors.New("Get"+" forward to backup fail")
	}

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	key := args.Key
	value := args.Value
	op := args.Op
	id := args.Id


	if !pb.isPrimary() {
		// if the current server is not primary,
		// then tell clients that wrong server error
		DPrintf(pb.me+" is not primary server. Current primary server is "+pb.currentView.Primary+". The '"+op+"' operation is invalid.")
		reply.Err = ErrWrongServer
		return errors.New(pb.me+" is not primary server. Current primary server is "+pb.currentView.Primary+". The '"+op+"' operation is invalid.")
	} 

	// if the current server is primary,
	// then it should forward this 'Op' to backup
	// if the operation has already been processed
	if pb.received[id] {

		reply.Err = OK
		return nil

	} 

	if pb.forwardOpToBackup(key,value,op,id) {
		// if backup did 'Get' successfully

		DPrintf("Server :"+pb.me+". "+op+" Key :"+key+" Value :"+value)

		switch op {
		case "Put":
			pb.database[key] = value
		case "Append":
			pb.database[key] = pb.database[key] + value
		}

		reply.Err = OK
		pb.received[id] = true

	} else {
		// if backup doesn't did 'Get' successfully,
		// the only reason would be that this server is not primary in the view of viewservice
		DPrintf(op+" forward to backup fail")
		reply.Err = ErrWrongServer
		return errors.New(op+" forward to backup fail")
	}

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.currentView.Viewnum)

	if err != nil {

	} else {

		needTransferToBackup := pb.isPrimary() && view.Backup != "" && view.Backup != pb.currentView.Backup

		pb.currentView = view

		if needTransferToBackup {
			pb.transferDatabaseToBackup()
		}		
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
	DPrintf(pb.me+" is killed")
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.currentView = viewservice.View{Primary:"",Backup:"",Viewnum:0}
	pb.database = make(map[string] string)
	pb.received = make(map[string] bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
