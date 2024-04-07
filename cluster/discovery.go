package cluster

import (
	"context"
	"encoding/json"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"net"
	consist "reids-by-go/datastruct/consisten"
	"time"
)

const (
	serverPrefixKey = "discovery&etcd/"
)

type Discovery struct {
	EtcdAddress    []string
	SrvInfos       map[string]*Server
	ConnMap        map[string]net.Conn
	ConsistentHash *consist.ConsistentHash
	DialTimeout    int

	closeCh   chan struct{}
	watchCh   clientv3.WatchChan
	cli       *clientv3.Client
	keyPrefix string
}

func NewDiscovery(etcdAddr []string, TimeOut int) (*Discovery, error) {
	var err error
	dis := &Discovery{
		EtcdAddress:    etcdAddr,
		DialTimeout:    TimeOut,
		closeCh:        make(chan struct{}),
		SrvInfos:       make(map[string]*Server),
		ConnMap:        make(map[string]net.Conn),
		ConsistentHash: consist.NewConsistentHash(3, nil),
	}
	dis.cli, err = clientv3.New(clientv3.Config{
		Endpoints:   dis.EtcdAddress,
		DialTimeout: time.Duration(dis.DialTimeout) * time.Second,
	})
	if err != nil {
		return nil, err
	}

	response, err := dis.cli.Get(context.Background(), serverPrefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range response.Kvs {
		var srvInfo Server
		_ = json.Unmarshal(kv.Value, &srvInfo)
		log.Println(string(kv.Key), string(kv.Value))
		dis.SrvInfos[string(kv.Key)] = &srvInfo
		dis.ConsistentHash.Add(string(kv.Key))
		conn, err1 := net.Dial("tcp", srvInfo.Addr)
		if err1 != nil {
			return nil, err1
		}
		dis.ConnMap[string(kv.Key)] = conn
	}
	dis.watchCh = dis.cli.Watch(context.Background(), serverPrefixKey, clientv3.WithPrefix())
	return dis, nil
}

func (d *Discovery) close() {
	d.closeCh <- struct{}{}
}

func (d *Discovery) watch() {
	for {
		select {
		case WatchResponse := <-d.watchCh:
			for _, event := range WatchResponse.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					log.Println("watch put", event.Kv)
					var srvInfo *Server
					_ = json.Unmarshal(event.Kv.Value, srvInfo)
					d.SrvInfos[string(event.Kv.Key)] = srvInfo
					d.ConsistentHash.Add(string(event.Kv.Key))
					conn, err := net.Dial("tcp", srvInfo.Addr)
					if err != nil {
						log.Println("watch add conn error")
						return
					}
					d.ConnMap[string(event.Kv.Key)] = conn

				case clientv3.EventTypeDelete:
					log.Println("watch delete", event.Kv)
					err := d.ConnMap[string(event.Kv.Key)].Close()
					if err != nil {
						log.Println("watch add conn close error")
						return
					}
					delete(d.SrvInfos, string(event.Kv.Key))
					d.ConsistentHash.Del(string(event.Kv.Key))
				}
			}
		}
	}
}
