package mqrpc

import (
	"time"
	"github.com/nats-io/nats"
	"testing"
)

type Args struct {
	A, B int
}
type Arith int
func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func TestRequest(t *testing.T) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Fatal("Cannot connect: ", err)
	}
	defer nc.Close()
	subj := "test_mqrpc"
	s, err := NewServer(nc, subj, "")
	if err != nil {
		t.Fatal("Cannot create server: ", err)
	}
	defer s.Close()
	arith := new(Arith)
	s.Register(arith)
	args := &Args{7,8}
	var reply int
	client := NewClient(nc, subj, 10 * time.Second)
	err = client.Call("Arith.Multiply", args, &reply)
	if err != nil {
		t.Fatal("Call error: ", err)
	}
	if reply != 56 {
		t.Fatal("Invalid reply: ", reply)
	}
}

