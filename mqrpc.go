package mqrpc

import (
	"sync"
	"reflect"

	"github.com/nats-io/nats"
)

// Precompute the reflect type for error. Can't use error directly
// because Typeof takes an empty interface value. This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	sync.Mutex // protects counters
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

// Request is a header written before every RPC call. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Request struct {
	ServiceMethod string   // format: "Service.Method"
	Seq           uint64   // sequence number chosen by client
	next          *Request // for free list in Server
}

// Response is a header written before every RPC return. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type Response struct {
	ServiceMethod string    // echoes that of the Request
	Seq           uint64    // echoes that of the request
	Error         string    // error, if any.
	next          *Response // for free list in Server
}


type MqRpc struct {
	sub *nats.Subscription

	subj string
	queue string
}

func (r *MqRpc) msghandler(msg *nats.Msg) {

}

func NewMqRpc(nc *nats.Conn, subj, queue string) (r *MqRpc, err error) {
	r = &MqRpc {
		subj: subj,
		queue: queue,
	}
	if queue != "" {
		r.sub, err = nc.QueueSubscribe(subj, queue, r.msghandler)
	} else {
		r.sub, err = nc.Subscribe(subj, r.msghandler)
	}
	if err != nil {
		return nil, err
	}
	return
}

func (r *MqRpc) Close() error {
	return r.sub.Unsubscribe()
}

func (r *MqRpc) Register(rcvr interface{}) error {
	return r.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (r *MqRpc) RegisterName(name string, rcvr interface{}) error {
	return r.register(rcvr, name, true)
}

func (r *MqRpc) register(rcvr interface{}, name string, useName bool) error {
	return nil//FIXME
}
