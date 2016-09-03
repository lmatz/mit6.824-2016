package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
// import "strconv"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	// where primary has ack
	ackByPrimary bool
	// current primary
	primary  string
	// current backup
	backup   string
	// current viewnum
	viewnum uint
	// current records of the liveness of clients
	liveness map[string] bool
	// times of ping issued by the clients
	lastPingTime map[string] time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	t := time.Now() 
	clerk := args.Me
	viewnum := args.Viewnum

	vs.liveness[clerk] = true
	vs.lastPingTime[clerk] = t


	vs.mu.Lock()
	defer vs.mu.Unlock()

	// check whether this is a ack from primary
	if vs.primary != "" && clerk == vs.primary && viewnum == vs.viewnum {
		vs.primaryAckCurrentView()
	}

	if vs.primary != "" && clerk == vs.primary && viewnum == 0 {
		vs.promoteBackupToPrimary()
		vs.promoteIdleToBackup()
		vs.moveToNextView()
	}


	if vs.primary == "" {
		vs.primary = clerk
		vs.moveToNextView()

	} else if vs.primary != "" && vs.backup == "" {

		if clerk != vs.primary {
			vs.backup = clerk
			vs.moveToNextView()
		}

	} else if vs.primary == "" && vs.backup != "" {

		log.Fatal("Impossible: primary is empty but backup is not")

	} else if vs.primary != "" && vs.backup != "" {

	}


	reply.View = View{Viewnum:vs.viewnum,Primary:vs.primary,Backup:vs.backup}

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = View{Viewnum:vs.viewnum,Primary:vs.primary,Backup:vs.backup}
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	now := time.Now()
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for clerk, then := range vs.lastPingTime {
		duration := now.Sub(then)
		DPrintf("duration: %f\n",duration.Seconds())
		DPrintf("interval: %f\n",(DeadPings * PingInterval.Seconds()))
		if duration.Seconds() > (DeadPings * PingInterval.Seconds()) {
			DPrintf("detect a fail server: "+clerk)
			vs.liveness[clerk] = false
		}
	}

	// primary fails and primary acked the cuurent, but there is no backup
	if vs.hasPrimary() && vs.primaryIsNotLive() && !vs.hasBackup() && vs.ackCurrentViewByPrimary() {
		// do nothing
	}

	// primary fails and primary acked the current view and there is a backup
	if vs.hasPrimary() && vs.primaryIsNotLive() && vs.hasBackup() && vs.ackCurrentViewByPrimary() {
		DPrintf("detect primary fails")
		vs.promoteBackupToPrimary()
		vs.promoteIdleToBackup()
		vs.moveToNextView()
	}

	// backup fails and primary acked the current view
	if vs.hasPrimary() && vs.hasBackup() && vs.backupIsNotLive() && vs.ackCurrentViewByPrimary() {
		DPrintf("detect backup fails")
		if vs.promoteIdleToBackup() {
			DPrintf("promote an idel to backup")
			vs.moveToNextView()
		}
	}

	// if backup is empty and primary acked the current view
	if vs.hasPrimary() && vs.primaryIsLive() && !vs.hasBackup() && vs.ackCurrentViewByPrimary() {
		DPrintf("detect backup is empty")
		if vs.promoteIdleToBackup() {
			DPrintf("promote an idel to backup")
			vs.moveToNextView()
		}
	}

}

func (vs *ViewServer) GetPrimary() string {
	return vs.primary
}

func (vs *ViewServer) GetBackup() string {
	return vs.backup
}

func (vs *ViewServer) hasPrimary() bool {
	return vs.primary != ""
}

func (vs *ViewServer) hasBackup() bool {
	return vs.backup != ""
}

func (vs *ViewServer) primaryIsLive() bool {
	return vs.liveness[vs.primary]
}

func (vs *ViewServer) backupIsLive() bool {
	return vs.liveness[vs.backup]
}

func (vs *ViewServer) primaryIsNotLive() bool {
	return !vs.primaryIsLive()
}

func (vs *ViewServer) backupIsNotLive() bool {
	return !vs.backupIsLive()
}

func (vs *ViewServer) moveToNextView() {
	vs.ackByPrimary = false
	vs.viewnum++
}

func (vs *ViewServer) promoteBackupToPrimary() {
	vs.primary = vs.backup
	vs.backup = ""
}

func (vs *ViewServer) promoteIdleToBackup() bool {
	vs.backup = ""
	for clerk, live := range vs.liveness {
		if vs.primary != clerk && vs.backup != clerk && live == true {
			vs.backup = clerk
			return true
		}
	}
	return false
}

func (vs *ViewServer) primaryAckCurrentView() {
	vs.ackByPrimary = true
}

func (vs *ViewServer) ackCurrentViewByPrimary() bool {
	return vs.ackByPrimary
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.

	vs.ackByPrimary = false
	vs.primary = ""
	vs.backup = ""
	vs.viewnum = 0
	vs.liveness = make(map[string] bool)
	vs.lastPingTime = make(map[string] time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
