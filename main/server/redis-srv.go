package main

import (
	"flag"
	"math/rand"
	"reids-by-go/cluster"
	"reids-by-go/config"
	"reids-by-go/redis/server"
	"reids-by-go/tcp"
	"reids-by-go/utils/port"
	"strconv"
	"time"
)

func main() {
	isCluster := flag.Bool("cluster", false, "is cluster mod")
	version := flag.String("version", "1.0", "the server's version")
	name := flag.String("name", "redis-server"+strconv.Itoa(int(time.Now().Unix()))+strconv.Itoa(rand.Intn(1000000)), "the server's version")
	flag.Parse()
	if *isCluster {
		err := tcp.ListenAndServerWithSignal(&tcp.Config{
			Server: &cluster.Server{
				Name:    *name,
				Addr:    "127.0.0.1:" + port.GetFreePort(),
				Version: *version,
			},
			DiscoveryConfig: &tcp.DiscoveryConfig{
				EtcdAddress: config.EtcdConfig.Addresses,
				DialTimeOut: config.EtcdConfig.DialTimeOut,
				TTL:         config.EtcdConfig.Ttl,
			},
		}, server.NewHandler())
		if err != nil {
			panic(err)
		}
	} else {
		err := tcp.ListenAndServerWithSignal(&tcp.Config{
			Server: &cluster.Server{
				Name:    *name,
				Addr:    "127.0.0.1:" + port.GetFreePort(),
				Version: *version,
			},
		}, server.NewHandler())
		if err != nil {
			panic(err)
		}
	}
}
