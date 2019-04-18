package mqrpc

import (
	"log"
	"bytes"
	"bufio"
	"encoding/gob"
	"net/rpc"

	"github.com/nats-io/nats"
)

type gobMqRpcCodec struct {
	dec *gob.Decoder
	enc *gob.Encoder
	writer *bufio.Writer
	closed bool
}

func (c *gobMqRpcCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.dec.Decode(r)
}

func (c *gobMqRpcCodec) ReadRequestBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *gobMqRpcCodec) WriteResponse(resp *rpc.Response, body interface{}) (err error) {
	if err = c.enc.Encode(resp); err != nil {
		if c.writer.Flush() == nil {
			// Gob couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			log.Println("mqrpc: gob error encoding response:", err)
			c.Close()
		}
		return
	}
	if err = c.enc.Encode(body); err != nil {
		if c.writer.Flush() == nil {
			// Was a gob problem encoding the body but the header has been written.
			// Shut down the connection to signal that the connection is broken.
			log.Println("mqrpc: gob error encoding body:", err)
			c.Close()
		}
		return
	}
	return c.writer.Flush()
}

func (c *gobMqRpcCodec) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return nil // nothing to do
}

type MqRpc struct {
	nc *nats.Conn
	sub *nats.Subscription
	srv *rpc.Server

	subj string
	queue string
}

func (r *MqRpc) msghandler(msg *nats.Msg) {
	outbuf := bytes.NewBuffer([]byte{})
	writer := bufio.NewWriter(outbuf)
	reader := bytes.NewReader(msg.Data)
	codec := gobMqRpcCodec{
		dec: gob.NewDecoder(reader),
		enc: gob.NewEncoder(writer),
		writer: writer,
		closed: false,
	}
	r.srv.ServeCodec(&codec)
	err := r.nc.Publish(msg.Reply, outbuf.Bytes())
	if err != nil {
		log.Println("mqrpc: nats Publish error:", err)
	}
}

func NewMqRpc(nc *nats.Conn, subj, queue string) (r *MqRpc, err error) {
	r = &MqRpc {
		nc: nc,
		subj: subj,
		queue: queue,
		srv: rpc.NewServer(),
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
	return r.srv.Register(rcvr)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (r *MqRpc) RegisterName(name string, rcvr interface{}) error {
	return r.srv.RegisterName(name, true)
}

