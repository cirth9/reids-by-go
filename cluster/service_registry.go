package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"log"
	"strings"
	"time"
)

type Server struct {
	Name    string `json:"Name"`
	Addr    string `json:"Addr"`
	Version string `json:"Version"`
}

type Registry struct {
	cli         *clientv3.Client
	EtcdAddress []string
	DialTimeout int

	closeCh     chan struct{}
	leasesID    clientv3.LeaseID
	keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse

	srvTTL  int64
	srvInfo *Server
}

func NewRegister(etcdAddress []string, timeOut int) *Registry {
	return &Registry{
		EtcdAddress: etcdAddress,
		DialTimeout: timeOut,
		closeCh:     make(chan struct{}),
	}
}

// Register a service
func (r *Registry) Register(srvInfo *Server, ttl int64) (chan<- struct{}, error) {
	var err error

	if strings.Split(srvInfo.Addr, ":")[0] == "" {
		return nil, errors.New("invalid ip")
	}

	if r.cli, err = clientv3.New(clientv3.Config{
		Endpoints:   r.EtcdAddress,
		DialTimeout: time.Duration(r.DialTimeout) * time.Second,
	}); err != nil {
		return nil, err
	}

	r.srvInfo = srvInfo
	r.srvTTL = ttl

	if err = r.registry(); err != nil {
		return nil, err
	}

	r.closeCh = make(chan struct{})

	go r.keepAlive()
	return r.closeCh, nil
}

func (r *Registry) Close() {
	r.closeCh <- struct{}{}
}

func (r *Registry) keepAlive() {
	ticker := time.NewTicker(time.Duration(r.srvTTL) * time.Second)
	for {
		select {
		case <-r.closeCh:
			if err := r.UnRegistry(); err != nil {
				log.Println("unregister failed", zap.Error(err))
			}
			if _, err := r.cli.Revoke(context.Background(), r.leasesID); err != nil {
				log.Println("revoke failed", zap.Error(err))
			}
			return
		case res := <-r.keepAliveCh:
			if res == nil {
				if err := r.UnRegistry(); err != nil {
					log.Println("register failed", zap.Error(err))
				}
			}
		case <-ticker.C:
			if r.keepAliveCh == nil {
				if err := r.UnRegistry(); err != nil {
					log.Println("register failed", zap.Error(err))
				}
			}
		}
	}
}

func (r *Registry) registry() error {
	leaseCtx, cancel := context.WithTimeout(context.Background(), time.Duration(r.DialTimeout)*time.Second)
	defer cancel()

	leaseResp, err := r.cli.Grant(leaseCtx, r.srvTTL)
	if err != nil {
		return err
	}
	r.leasesID = leaseResp.ID
	if r.keepAliveCh, err = r.cli.KeepAlive(context.Background(), leaseResp.ID); err != nil {
		return err
	}

	data, err := json.Marshal(r.srvInfo)
	if err != nil {
		return err
	}
	_, err = r.cli.Put(context.Background(), BuildRegPath(r.srvInfo), string(data), clientv3.WithLease(r.leasesID))
	return err
}

func (r *Registry) UnRegistry() error {
	_, err := r.cli.Delete(context.Background(), BuildRegPath(r.srvInfo))
	return err
}

func BuildRegPath(srvInfo *Server) string {
	return fmt.Sprintf("%v/%v/%v/%v", serverPrefixKey, srvInfo.Addr, srvInfo.Name, srvInfo.Version)
}
