package tcp

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"reids-by-go/cluster"
	"reids-by-go/interface/tcp"
	"sync"
	"syscall"
	"time"
)

type DiscoveryConfig struct {
	EtcdAddress []string `yaml:"etcd-address"`
	DialTimeOut int      `yaml:"dial-time-out"`
	TTL         int64    `yaml:"ttl"`
}

type Config struct {
	MaxConnect       uint32        `yaml:"max-connect"`
	Timeout          time.Duration `yaml:"timeout"`
	*cluster.Server  `yaml:"srv-info"`
	*DiscoveryConfig `yaml:"discovery-config"`
}

var ServerConfig *Config

// ListenAndServer   监听并且提供服务，收到close chan后关闭
func ListenAndServer(listener net.Listener, handler tcp.Handler, closeChan chan struct{}, register *cluster.Registry) {
	go func() {
		<-closeChan
		log.Println("shut down...")
		if register != nil {
			err := register.UnRegistry()
			if err != nil {
				panic(err)
			}
			log.Println("unRegistry...")
			register.Close()
		}
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var wg sync.WaitGroup
	for {
		conn, err1 := listener.Accept()
		if err1 != nil {
			break
		}
		log.Println("accept link")
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
}

func ListenAndServerWithSignal(cfg *Config, handler tcp.Handler) error {
	ServerConfig = cfg
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}

	var register *cluster.Registry = nil
	if cfg.DiscoveryConfig != nil {
		var err1 error
		register = cluster.NewRegister(cfg.EtcdAddress, cfg.DialTimeOut)
		_, err1 = register.Register(cfg.Server, cfg.TTL)
		if err1 != nil {
			log.Println("discovery error ", err1.Error())
			return err1
		}
	}
	log.Println(fmt.Sprintf("bind: %s,start listening...", cfg.Addr))
	ListenAndServer(listener, handler, closeChan, register)
	return nil
}
