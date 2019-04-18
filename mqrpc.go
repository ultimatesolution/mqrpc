package mqrpc

import (
	"time"
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
	return nil // nothing to do
}

type MqRpcServer struct {
	nc *nats.Conn
	sub *nats.Subscription
	srv *rpc.Server

	subj string
	queue string
}

func (r *MqRpcServer) msghandler(msg *nats.Msg) {
	outbuf := bytes.NewBuffer([]byte{})
	writer := bufio.NewWriter(outbuf)
	reader := bytes.NewReader(msg.Data)
	codec := gobMqRpcCodec{
		dec: gob.NewDecoder(reader),
		enc: gob.NewEncoder(writer),
		writer: writer,
	}
	r.srv.ServeCodec(&codec)
	err := r.nc.Publish(msg.Reply, outbuf.Bytes())
	if err != nil {
		log.Println("mqrpc: nats Publish error:", err)
	}
}

func NewServer(nc *nats.Conn, subj, queue string) (r *MqRpcServer, err error) {
	r = &MqRpcServer {
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

func (r *MqRpcServer) Close() error {
	return r.sub.Unsubscribe()
}

func (r *MqRpcServer) Register(rcvr interface{}) error {
	return r.srv.Register(rcvr)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (r *MqRpcServer) RegisterName(name string, rcvr interface{}) error {
	return r.srv.RegisterName(name, true)
}

type gobMqClientCodec struct {
	nc *nats.Conn
	subj string
	timeout time.Duration
	replyReader *bytes.Reader
	dec *gob.Decoder
	sync chan bool
}

func (c *gobMqClientCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	outbuf := bytes.NewBuffer([]byte{})
	writer := bufio.NewWriter(outbuf)
	enc := gob.NewEncoder(writer)
	if err := enc.Encode(r); err != nil {
		return err
	}
	if err := enc.Encode(body); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		log.Println("client WriteRequest Flush error:", err)
		return err
	}
	msg, err := c.nc.Request(c.subj, outbuf.Bytes(), c.timeout)
	if err != nil {
		log.Println("client WriteRequest Request error:", err)
		return err
	}
	// Create reply reader inside codec. It will be used later in Read*()
	c.replyReader = bytes.NewReader(msg.Data)
	c.dec = gob.NewDecoder(c.replyReader)
	c.sync <- true
	return nil
}

func (c *gobMqClientCodec) ReadResponseHeader(r *rpc.Response) error {
	<- c.sync
	return c.dec.Decode(r)
}

func (c *gobMqClientCodec) ReadResponseBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c *gobMqClientCodec) Close() error {
	return nil // nothing to do
}

func NewClient(nc *nats.Conn, subj string, timeout time.Duration) *rpc.Client {
	codec := gobMqClientCodec{
		nc: nc,
		subj: subj,
		timeout: timeout,
		sync: make(chan bool),
	}
	return rpc.NewClientWithCodec(&codec)
}
