package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

	view    View
	actions map[string]time.Time
	ack     map[string]uint
	rfc     bool // ready for change
}

//
// make a new view, effectively increase
// viewNum and make rfc become false
// not threadsafe. somebody needs to lock me!
//
func (vs *ViewServer) NewView() {
	vs.view.Viewnum += 1
	vs.rfc = false
}

//
// add server s to the system
// not threadsafe. somebody needs to lock me!
//
func (vs *ViewServer) AddServer(s string) {
	vs.actions[s] = time.Now()
	if vs.rfc {
		if vs.view.Primary == "" {
			vs.view.Primary = s
			vs.NewView()
		}
		if vs.view.Primary != s && vs.view.Backup == "" {
			vs.view.Backup = s
			vs.NewView()
		}
	}
}

//
// remove server s from the system
// not threadsafe. somebody needs to lock me!
//
func (vs *ViewServer) RemoveServer(s string) {
	pick := func() string {
		for k, _ := range vs.actions {
			if k != vs.view.Primary && k != vs.view.Backup {
				return k
			}
		}
		return ""
	}
	if vs.rfc {
		p := pick()
		if vs.view.Primary == s {
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = p
			vs.NewView()
		} else if vs.view.Backup == s {
			vs.view.Backup = p
			vs.NewView()
		}
	}
	delete(vs.ack, s)
	delete(vs.actions, s)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	if args.Viewnum < vs.ack[args.Me] {
		vs.RemoveServer(args.Me)
	}
	vs.ack[args.Me] = args.Viewnum
	if args.Me == vs.view.Primary && args.Viewnum == vs.view.Viewnum {
		vs.rfc = true
	}
	vs.AddServer(args.Me)
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	for k, v := range vs.actions {
		if v.Add(PingInterval * DeadPings).Before(time.Now()) {
			vs.RemoveServer(k)
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me

	vs.view = View{0, "", ""}
	vs.actions = make(map[string]time.Time)
	vs.ack = make(map[string]uint)
	vs.rfc = true

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
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
