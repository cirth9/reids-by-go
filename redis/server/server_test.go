package server

import (
	"reids-by-go/cluster"
	"reids-by-go/tcp"
	"reids-by-go/utils/port"
	"testing"
)

func TestServer(t *testing.T) {
	err := tcp.ListenAndServerWithSignal(&tcp.Config{
		Server: &cluster.Server{
			Name:    "redis_server",
			Addr:    "127.0.0.1:" + port.GetFreePort(),
			Version: "1.0",
		},
		DiscoveryConfig: &tcp.DiscoveryConfig{
			EtcdAddress: []string{"127.0.0.1:2379"},
			DialTimeOut: 1000000000,
			TTL:         1000000000,
		},
	}, NewHandler())
	if err != nil {
		panic(err)
	}
}
