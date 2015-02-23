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
// px = paxos.Make(peers []string, me string)
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
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = 1
	Pending        = 2 // not yet decided.
	Forgotten      = 3 // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	gen  map[int]int64
	n_p  map[int]int64
	n_a  map[int]int64
	v_a  map[int]interface{}
	dec  map[int]interface{}
	done int
	max  int
	min  int
}

type PrepareArgs struct {
	Seq int
	N   int64
}

type PrepareReply struct {
	Ok bool
	N  int64
	V  interface{}
}

type AcceptArgs struct {
	Seq int
	N   int64
	V   interface{}
}

type AcceptReply struct {
	Ok bool
}

type DecidedArgs struct {
	Seq int
	V   interface{}
}

type DecidedReply struct {
	Done int
}

type PurgeArgs struct {
	Done int
}

type NoReply struct {
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

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go px.Propose(seq, v)
}

func (px *Paxos) GenN(seq int) int64 {
	px.mu.Lock()
	npeers := int64(len(px.peers))
	maxx := px.n_p[seq]
	if maxx < px.gen[seq] {
		maxx = px.gen[seq]
	}
	n := (maxx/npeers+1)*npeers + int64(px.me)
	px.gen[seq] = n
	px.mu.Unlock()
	return n
}

func (px *Paxos) Propose(seq int, v interface{}) {
	for {
		px.mu.Lock()
		if _, ok := px.dec[seq]; ok {
			px.mu.Unlock()
			break
		}
		px.mu.Unlock()
		n := px.GenN(seq)
		n_a := int64(-1)
		v_a := v
		i := 0
		for idx := range px.peers {
			peer := px.peers[idx]
			args := PrepareArgs{seq, n}
			reply := &PrepareReply{}
			var success bool
			if idx == px.me {
				success = px.Prepare(&args, reply) == nil
			} else {
				success = call(peer, "Paxos.Prepare", args, reply)
			}
			if success && reply.Ok {
				i = i + 1
				if reply.N > n_a {
					n_a = reply.N
					v_a = reply.V
				}
			}
		}
		if i > len(px.peers)/2 {
			ii := 0
			for idx := range px.peers {
				peer := px.peers[idx]
				args := AcceptArgs{seq, n, v_a}
				reply := &AcceptReply{}
				var success bool
				if idx == px.me {
					success = px.Accept(&args, reply) == nil
				} else {
					success = call(peer, "Paxos.Accept", args, reply)
				}
				if success && reply.Ok {
					ii = ii + 1
				}
			}
			if ii > len(px.peers)/2 {
				m := px.done
				r := true
				for idx := range px.peers {
					reply := &DecidedReply{}
					args := DecidedArgs{seq, v_a}
					if idx == px.me {
						r = (px.Decided(&args, reply) == nil) && r
					} else {
						r = call(px.peers[idx], "Paxos.Decided", args, reply) && r
					}
					if reply.Done < m {
						m = reply.Done
					}
				}
				if r {
					for idx := range px.peers {
						args := PurgeArgs{m}
						if idx == px.me {
							px.Purge(&args, &NoReply{})
						} else {
							call(px.peers[idx], "Paxos.Purge", args, &NoReply{})
						}
					}
				}
			}
		}
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	if args.N > px.n_p[args.Seq] {
		px.n_p[args.Seq] = args.N
		reply.Ok = true
		if _, ok := px.v_a[args.Seq]; ok {
			reply.N = px.n_a[args.Seq]
			reply.V = px.v_a[args.Seq]
		} else {
			reply.N = -1
		}
	} else {
		reply.Ok = false
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	if args.N >= px.n_p[args.Seq] {
		px.n_p[args.Seq] = args.N
		px.n_a[args.Seq] = args.N
		px.v_a[args.Seq] = args.V
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	px.dec[args.Seq] = args.V
	if px.max < args.Seq {
		px.max = args.Seq
	}
	reply.Done = px.done
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Purge(args *PurgeArgs, reply *NoReply) error {
	px.mu.Lock()
	px.min = args.Done + 1
	for key := range px.n_p {
		if key < px.min {
			delete(px.n_p, key)
			delete(px.n_a, key)
			delete(px.v_a, key)
			delete(px.dec, key)
			delete(px.gen, key)
		}
	}
	px.mu.Unlock()
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	if seq > px.done {
		px.done = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.max
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
	return px.min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	if seq <= px.done {
		px.mu.Unlock()
		return Forgotten, nil
	}
	if v, ok := px.dec[seq]; ok {
		px.mu.Unlock()
		return Decided, v
	}
	px.mu.Unlock()
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
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

	px.gen = make(map[int]int64)
	px.n_p = make(map[int]int64)
	px.n_a = make(map[int]int64)
	px.v_a = make(map[int]interface{})
	px.dec = make(map[int]interface{})
	px.done = -1
	px.max = 0
	px.min = 0

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
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
