package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"reids-by-go/config"
	"reids-by-go/interface/redis"
	redisClient "reids-by-go/redis/client"
	"reids-by-go/redis/protocol"
	"strings"
)

var (
	client *redisClient.Client
	err    error
)

func main() {
	isCluster := flag.Bool("cluster", false, "is cluster mod")
	redisAddr := flag.String("redis_addr", "127.0.0.1:9999", "redis address")
	flag.Parse()
	log.Println(*redisAddr)
	if *isCluster {
		client, err = redisClient.MakeClusterClient(&redisClient.Config{
			EtcdAddr:    config.EtcdConfig.Addresses,
			DialTimeOut: config.EtcdConfig.DialTimeOut,
		})
		if err != nil {
			panic(err.Error())
			return
		}
	} else {
		client, err = redisClient.MakeSingleClient(*redisAddr)
		if err != nil {
			panic(err.Error())
			return
		}
	}
	client.Start()
	fmt.Println("Welcome to redis!")
	if *isCluster {
		fmt.Printf("etcd %v >", config.EtcdConfig.DialTimeOut)
	} else {
		fmt.Printf("%s >", *redisAddr)
	}
	for {
		var cmdline string
		var args [][]byte
		reader := bufio.NewReader(os.Stdin)
		line, _, err2 := reader.ReadLine()
		cmdline = string(line)
		if err2 != nil {
			panic(err2.Error())
		}
		log.Println(cmdline)
		if err != nil {
			log.Println("CMD PARSE ERROR")
			continue
		}
		split := strings.Split(cmdline, " ")
		for i := 0; i < len(split); i++ {
			if split[i] == " " {
				continue
			}
			args = append(args, []byte(split[i]))
		}
		reply := client.Send(args)
		ParseReply(reply)
		if *isCluster {
			fmt.Printf("etcd %v >", config.EtcdConfig.DialTimeOut)
		} else {
			fmt.Printf("%s >", *redisAddr)
		}
	}
}

func ParseReply(reply redis.Reply) {
	if parseReply, ok := reply.(*protocol.ErrReply); ok {
		fmt.Println(parseReply.Error())
		return
	} else if parseReply, ok := reply.(*protocol.IntReply); ok {
		fmt.Println(parseReply.Value)
		return
	} else if parseReply, ok := reply.(*protocol.TimeReply); ok {
		fmt.Println(parseReply.Time)
		return
	} else if parseReply, ok := reply.(*protocol.MultiBulkStringReply); ok {
		for i := 0; i < len(parseReply.Args); i++ {
			fmt.Print(string(parseReply.Args[i]) + " ")
		}
		fmt.Println()
		return
	} else if parseReply, ok := reply.(*protocol.FloatReply); ok {
		fmt.Println(parseReply.Value)
		return
	} else if parseReply, ok := reply.(*protocol.BulkReply); ok {
		fmt.Println(string(parseReply.Arg))
		return
	} else if parseReply, ok := reply.(*protocol.StatusReply); ok {
		fmt.Println(parseReply.Status)
		return
	}
}
