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
// px = paxos.Make(peers []string, me int)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
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
import "sync/atomic"
import "fmt"
import "math/rand"
import "math"
import "time"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const Debug = 0


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}


type PaxosInstance struct {
	N_P        int64
	N_A        int64
	V        interface{}
	Status     Fate
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	
	// for proposer:
	// the log entry for each instance not deleted
	instances          map[int] *PaxosInstance

	// seq <= done is already decided
	done			   []int

	maxInstanceKnown   int
}

type PrepareArg struct {
	InstanceNum   int
	Me    int
	Done  int
	Round  int64
}

type PrepareReply struct {
	Ok     bool
	InstanceNum   int
	Me     int
	Done   int
	Round  int64
	V    interface{}
}

type AcceptArg struct {
	InstanceNum   int
	Me     int
	Done   int
	Round  int64
	V      interface{}
}

type AcceptReply struct {
	Ok     bool
	InstanceNum   int
	Me     int
	Done   int
	Round  int64
}

type DecidedArg struct {
	InstanceNum   int
	Me     int
	Done   int
	V      interface{}
}

type DecidedReply struct {

}


func NewPaxosInstance() *PaxosInstance {
	res := new(PaxosInstance)
	res.N_P = -1
	res.N_A = -1
	res.Status = Pending
	res.V = nil
	return res
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


// proposer(v):
func (px *Paxos) proposer(seq int, v interface{}) {

	DPrintf("%d starts to prepare %d with %v\n",px.me, seq, v)

	// refresh the max instance number known by this peer
	px.mu.Lock()
	if ( seq > px.maxInstanceKnown ) {
		px.maxInstanceKnown = seq
	}
	px.mu.Unlock()

	// since round numbers must be unique and roughly follow time
	// just use the time
	round := time.Now().UnixNano()

	// if this 'seq' instance doesn't exists
	// then make a new one
	if _, exists := px.instances[seq] ; !exists && seq >= px.Min() {
		px.instances[seq] = NewPaxosInstance()
	}

	// otherwise, send prepare message to peers
	ok, n_a, v_a := px.prepare(seq, round)

	// if not ok from majority
	// do nothing
	if !ok {
		DPrintf("%d prepared %d, does't get ok from majority\n", px.me, seq)
		return
	}

	DPrintf("%d prepared %d, get ok from majority with n_a %d valued with %v\n", px.me, seq, n_a, v_a)

	// if prepare_ok from majority
	if n_a == -1 {
		// if no peers has ever accept a value
		// then we can choose our own value, v

	} else {
		// if there are peers that have already accepted a value
		v = v_a
	}

	DPrintf("%d starts to send accept %d with value %v\n", px.me, seq, v)

	ok = px.accept(seq, round, v)

	// if not accepted by the majority
	// do nothing
	if !ok {
		DPrintf("%d accept %d, doesn't get ok from majority\n", px.me, seq)
		return
	}

	DPrintf("%d accept %d, get ok from majority\n", px.me, seq)

	// if accepted by the majority
	// send decided(v) to all peers including itself
	px.decided(seq, v)

	DPrintf("%d send decided on %d to all\n", px.me, seq)
}

// decided(v):
func (px *Paxos) decided(seq int, v interface{}) {
	numberOfServers := len(px.peers)
	replies := make([]chan *DecidedReply, numberOfServers)
	for i, _ := range replies {
		replies[i] = make(chan *DecidedReply)
	}

	decidedArg := DecidedArg{InstanceNum:seq, V:v, Me:px.me, Done:px.done[px.me]}
	
	for i := 0; i<numberOfServers; i++ {
		decidedReply := new(DecidedReply)
		if i == px.me {
			go func(reply chan *DecidedReply) {
				px.DecidedHandler(decidedArg, decidedReply)
				reply <- decidedReply
			}(replies[i])
		} else {
			go func(reply chan *DecidedReply, index int) {
				call(px.peers[index], "Paxos.DecidedHandler", decidedArg, nil)
				reply <- new(DecidedReply)
			}(replies[i], i)
		}
	}

	for i := 0; i<numberOfServers; i++ {
		<- replies[i]
	}
}

func (px *Paxos) DecidedHandler(args DecidedArg, reply *DecidedReply) error {
	instanceNum := args.InstanceNum
	peer := args.Me
	v := args.V
	done := args.Done

	px.mu.Lock()
	defer px.mu.Unlock()

	if ( instanceNum > px.maxInstanceKnown ) {
		px.maxInstanceKnown = instanceNum
	}

	if done > px.done[peer]   {
		px.done[peer] = done
	}

	if  _, exists := px.instances[instanceNum] ; !exists {
		px.instances[instanceNum] = NewPaxosInstance()
	}

	px.instances[instanceNum].Status = Decided
	px.instances[instanceNum].V = v

	return nil
}


// prepare(n):
func (px *Paxos) prepare(instanceNum int, round int64) (bool, int64, interface{}) {
	numberOfServers := len(px.peers)
	replies := make([]chan *PrepareReply, numberOfServers)
	for i, _ := range replies {
		replies[i] = make(chan *PrepareReply)
	}

	// count how many peers including itself says yes
	count := 0
	// highest n_a from peers
	var n_a int64 = -1
	var v_a interface{}

	prepareArg := PrepareArg{ InstanceNum:instanceNum, Done:px.done[px.me], Me:px.me, Round:round }

	for i := 0; i<numberOfServers; i++ {

		prepareReply := new(PrepareReply)
		prepareReply.Ok = false

		// since one server need to send prepare to all of the peers including itself
		if i == px.me {
			go func(reply chan *PrepareReply) {
					px.PrepareHandler(prepareArg, prepareReply)
					reply <- prepareReply
				}(replies[i])
		} else {
			go func(reply chan *PrepareReply, peer string) {
				call(peer, "Paxos.PrepareHandler", prepareArg, prepareReply)
				reply <- prepareReply
			}(replies[i], px.peers[i])
		}
	}

	for i := 0; i<numberOfServers; i++ {
		reply := <- replies[i]
		if reply.Ok {
			count++
			if reply.Round > n_a {
				n_a = reply.Round
				v_a = reply.V
			}
		}
		DPrintf("!\n")
	}

	DPrintf("%d is preparing, get %d ok from all\n", px.me, count)
	if count > numberOfServers/2 {
		return true, n_a, v_a
	}

	return false, n_a, v_a
}



// PrepareHandler(n):
func (px *Paxos) PrepareHandler(args PrepareArg, reply *PrepareReply) error {
	instanceNum := args.InstanceNum
	round := args.Round
	peer := args.Me
	done := args.Done

	px.mu.Lock()
	defer px.mu.Unlock()

	DPrintf("%d is handling prepare from peer %d on %d\n", px.me, peer, round)



	if ( instanceNum > px.maxInstanceKnown ) {
		px.maxInstanceKnown = instanceNum
	}

	// refresh the largest instance number done by peer
	if px.done[peer] < done {
		px.done[peer] = done
	}

	if  _, exists := px.instances[instanceNum] ; !exists {
		px.instances[instanceNum] = NewPaxosInstance()
	}

	if  round > px.instances[instanceNum].N_P {
		px.instances[instanceNum].N_P = round

		reply.Ok = true
		reply.Round = px.instances[instanceNum].N_A
		reply.V = px.instances[instanceNum].V
	} else {
		reply.Ok = false
	}

	return nil
}

// accept(n, v):
func (px *Paxos) accept(instanceNum int, round int64, v interface{}) bool {
	numberOfServers := len(px.peers)

	replies := make([]chan *AcceptReply, numberOfServers)
	for i, _ := range replies {
		replies[i] = make(chan *AcceptReply)
	}
	// count how many peers including itself says yes
	count := 0

	acceptArg := AcceptArg{InstanceNum:instanceNum, V:v, Me:px.me, Done:px.done[px.me], Round:round }

	for i := 0; i<numberOfServers; i++ {

		acceptReply := new(AcceptReply)
		acceptReply.Ok = false
		
		if i == px.me {
			go func(reply chan *AcceptReply) {
				px.AcceptHandler(acceptArg,acceptReply)
				reply <- acceptReply
			}(replies[i])
		} else {
			go func(reply chan *AcceptReply, peer string) {
				call(peer, "Paxos.AcceptHandler", acceptArg, acceptReply)
				reply <- acceptReply
			}(replies[i], px.peers[i])
		}
	}

	for i := 0; i<numberOfServers; i++ {
		reply := <- replies[i]
		if reply.Ok {
			count++
		}
	}

	DPrintf("%d get %d accept_ok from all\n", px.me, count)

	if count > numberOfServers/2 {
		return true
	}

	return false
}

// AcceptHandler(n,v):
func (px *Paxos) AcceptHandler(args AcceptArg, reply *AcceptReply) error {
	instanceNum := args.InstanceNum
	v := args.V
	peer := args.Me
	done := args.Done
	round := args.Round

	px.mu.Lock()
	defer px.mu.Unlock()

	if ( instanceNum > px.maxInstanceKnown ) {
		px.maxInstanceKnown = instanceNum
	}

	// refresh the largest instance number done by peer
	if px.done[peer] < done {
		px.done[peer] = done
	}

	if  _, exists := px.instances[instanceNum] ; !exists {
		px.instances[instanceNum] = NewPaxosInstance()
	}

	DPrintf("%d is handling accept on instance %d with N_P %d and round %d\n", px.me, instanceNum, px.instances[instanceNum].N_P, round)

	if round >= px.instances[instanceNum].N_P {
		px.instances[instanceNum].N_P = round
		px.instances[instanceNum].N_A = round
		px.instances[instanceNum].V = v
		reply.Ok = true
	} else {
		reply.Ok = false
	}

	return nil
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
	status, _ := px.Status(seq)

	DPrintf("%d peer is starting %d with status %d\n",px.me, seq, status)

	if status != Decided && status != Forgotten {
		go px.proposer(seq,v)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if px.done[px.me] < seq { 
		px.done[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.maxInstanceKnown
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
	px.mu.Lock()
	defer px.mu.Unlock()

	res := math.MaxInt32

	for _, val := range px.done {
		if val < res {
			res = val
		}
	}

	for key, _ := range px.instances {
		if key < res+1 {
			delete(px.instances, key)
		}
	}

	return res+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	min := px.Min()

	DPrintf("%d 's status is min with %d\n", px.me, min)

	if seq < min {
		DPrintf("%d 's status is forgotten with %d\n", px.me, seq)
		return Forgotten, nil
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	if val, exists := px.instances[seq] ; exists {
		if val.Status == Pending {
			DPrintf("%d 's status is pending with %d\n", px.me, seq)
			go px.proposer(seq,"")
			return Pending, nil
		} else if val.Status == Decided {
			DPrintf("%d 's status is decided with %d valued with %v\n", px.me, seq, val.V)
			return Decided, val.V
		}
	}


	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
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
	px.maxInstanceKnown = -1
	px.instances = make(map[int] *PaxosInstance)
	px.done = make([]int, len(peers))

	for i := 0; i< len(px.done); i++ {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
