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

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	storage map[string]string
	action  map[int64]string
	view    viewservice.View
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else {
		reply.Err = OK
		reply.Value = pb.storage[args.Key]
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
	} else {
		for {
			reply.Err = OK
			ok := true
			if pb.view.Backup != "" {
				ok = call(pb.view.Backup, "PBServer.ForwardPutAppend", args, reply)
			}
			if ok && reply.Err == OK {
				if _, ok := pb.action[args.Id]; !ok {
					pb.action[args.Id] = pb.storage[args.Key]
					if args.IsPut {
						pb.storage[args.Key] = args.Value
					} else {
						pb.storage[args.Key] = pb.storage[args.Key] + args.Value
					}
				}
				break
			}
			time.Sleep(viewservice.PingInterval)
			pb.refreshView()
		}
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) ForwardPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
	} else {
		reply.Err = OK
		if _, ok := pb.action[args.Id]; !ok {
			pb.action[args.Id] = pb.storage[args.Key]
			if args.IsPut {
				pb.storage[args.Key] = args.Value
			} else {
				pb.storage[args.Key] = pb.storage[args.Key] + args.Value
			}
		}
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Migrate(args *MigrateArgs, reply *MigrateReply) error {
	pb.mu.Lock()
	pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
	if pb.view.Backup == pb.me {
		pb.storage = args.Storage
		pb.action = args.Action
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) refreshView() {
	v, _ := pb.vs.Ping(pb.view.Viewnum)
	if v != pb.view {
		if v.Primary == pb.me && v.Backup != "" {
			args := &MigrateArgs{pb.storage, pb.action}
			for {
				var reply MigrateReply
				ok := call(v.Backup, "PBServer.Migrate", args, &reply)
				if ok && reply.Err == OK {
					break
				}
				time.Sleep(viewservice.PingInterval)
			}
		}
	}
	pb.view = v
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	pb.refreshView()
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

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
	pb.storage = make(map[string]string)
	pb.action = make(map[int64]string)
	pb.view = viewservice.View{0, "", ""}

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
