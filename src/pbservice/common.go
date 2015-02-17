package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	IsPut bool
	Id    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	Id  int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Storage map[string]string
	Action  map[int64]string
}

type MigrateReply struct {
}
