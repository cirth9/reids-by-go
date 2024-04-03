package client

import (
	"context"
	"log"
	"net"
	"reids-by-go/interface/redis"
	"reids-by-go/lib/sync/wait"
	"reids-by-go/redis/parse"
	"reids-by-go/redis/protocol"
	"sync"
	"time"
)

const (
	chanSize = 256
	maxWait  = 10 * time.Second
)

type Request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartBeat bool
	waiting   *wait.Wait
	err       error
}

type Client struct {
	conn            net.Conn
	pendingRequests chan *Request
	waitingRequests chan *Request
	ticker          *time.Ticker
	addr            string

	ctx        context.Context
	cancelFunc context.CancelFunc
	working    *sync.WaitGroup
}

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		addr:            addr,
		conn:            conn,
		pendingRequests: make(chan *Request, chanSize),
		waitingRequests: make(chan *Request, chanSize),
		ctx:             ctx,
		cancelFunc:      cancel,
		working:         &sync.WaitGroup{},
	}, nil
}

func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go func() {
		err := client.handleRead()
		log.Print(err)
	}()
	go client.heartbeat()
}

func (client *Client) Send(args [][]byte) redis.Reply {
	var request = &Request{
		args:      args,
		heartBeat: false,
		waiting:   &wait.Wait{},
	}

	//log.Println("send args:", utils.BytesToStrings(request.args))

	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingRequests <- request
	timeout := request.waiting.WaitIfTimeOut(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if request.err != nil {
		return protocol.MakeErrReply("request failed")
	}
	//log.Println("send reply,", string(request.reply.(*protocol.BulkReply).Arg))
	return request.reply
}

func (client *Client) handleWrite() {
	for request := range client.pendingRequests {
		client.doRequest(request)
	}
}

func (client *Client) doRequest(request *Request) {
	if request == nil || len(request.args) == 0 {
		return
	}
	reply := protocol.MakeMultiBulkReply(request.args)
	bytes := reply.ToBytes()
	//log.Println(string(bytes))
	_, err := client.conn.Write(bytes)
	i := 0
	for err != nil && i < 3 {
		err = client.handleConnError(err)
		if err == nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		client.waitingRequests <- request
	} else {
		request.err = err
		request.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {

		}
	}()

	request := <-client.waitingRequests
	if request == nil {
		return
	}

	request.reply = reply
	//log.Println("reply,", string(request.reply.ToBytes()))
	//log.Println("finish request reply,", string(request.reply.(*protocol.StatusReply).Status))
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() error {
	ch := parse.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			client.finishRequest(protocol.MakeErrReply(payload.Err.Error()))
			//log.Println(payload.Err)
			continue
		}
		//reply := payload.Data.(*protocol.StatusReply)
		//log.Println("read reply: >>> \n", string(reply.ToBytes()))
		client.finishRequest(payload.Data)
	}
	return nil
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) doHeartbeat() {
	request := &Request{
		args:      [][]byte{[]byte("PING")},
		heartBeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingRequests <- request
	request.waiting.WaitIfTimeOut(maxWait)
}

func (client *Client) handleConnError(err error) error {
	return nil
}
