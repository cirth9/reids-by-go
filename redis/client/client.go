package client

import (
	"context"
	"log"
	"net"
	"reids-by-go/cluster"
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

type Config struct {
	EtcdAddr    []string
	DialTimeOut int
}

type Client struct {
	singleConn      net.Conn
	pendingRequests chan *Request
	waitingRequests chan *Request
	ticker          *time.Ticker
	addr            string
	discovery       *cluster.Discovery
	isCluster       bool

	ctx        context.Context
	cancelFunc context.CancelFunc
	working    *sync.WaitGroup
}

func MakeSingleClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		addr:            addr,
		singleConn:      conn,
		pendingRequests: make(chan *Request, chanSize),
		waitingRequests: make(chan *Request, chanSize),
		ctx:             ctx,
		cancelFunc:      cancel,
		isCluster:       false,
		working:         &sync.WaitGroup{},
	}
	return client, nil
}

func MakeClusterClient(config *Config) (*Client, error) {
	discovery, err := cluster.NewDiscovery(config.EtcdAddr, config.DialTimeOut)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		pendingRequests: make(chan *Request, chanSize),
		waitingRequests: make(chan *Request, chanSize),
		discovery:       discovery,
		ctx:             ctx,
		cancelFunc:      cancel,
		isCluster:       true,
		working:         &sync.WaitGroup{},
	}
	return client, nil
}

func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	if client.isCluster {

	} else {
		go func() {
			err := client.handleSingleRead()
			log.Print(err)
		}()
		go client.heartbeat()
	}
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
	if client.isCluster {
		client.doRequestByCluster(request, bytes)
	} else {
		client.doRequestBySingle(request, bytes)
	}
}

func (client *Client) doRequestBySingle(request *Request, bytes []byte) {
	_, err := client.singleConn.Write(bytes)
	i := 0
	for err != nil && i < 3 {
		err = client.handleConnError(err)
		if err == nil {
			_, err = client.singleConn.Write(bytes)
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

// todo 集群请求，对于所有的命令来讲第二个参数都是key，通过key可以确定唯一一个conn，并且进行操作
func (client *Client) doRequestByCluster(request *Request, bytes []byte) {
	//todo mset mget 等分散到多节点的多key命令
	singleCmds := cluster.MultipleToSingleCmd(request.args)

	key := string(request.args[1])
	get := client.discovery.ConsistentHash.Get(key)
	conn := client.discovery.ConnMap[get]
	_, err := conn.Write(bytes)
	i := 0
	for err != nil && i < 3 {
		err = client.handleConnError(err)
		if err == nil {
			_, err = conn.Write(bytes)
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
			log.Println(err)
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

func (client *Client) handleSingleRead() error {
	ch := parse.ParseStream(client.singleConn)
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
