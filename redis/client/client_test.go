package client

import (
	"fmt"
	"log"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"strings"
	"testing"
)

type Test func(redis.Reply)

func TestClient(t *testing.T) {
	client, err := MakeClusterClient(&Config{
		EtcdAddr:    []string{"127.0.0.1:2379"},
		DialTimeOut: 1000000000,
	})
	if err != nil {
		t.Error(err)
	}
	client.Start()
	result := client.Send([][]byte{
		[]byte("set"),
		[]byte("l1"),
		[]byte("v1"),
	})
	t.Log(string(result.ToBytes()))

	result = client.Send([][]byte{
		[]byte("set"),
		[]byte("a"),
		[]byte("b"),
	})
	t.Log(string(result.ToBytes()))

	//result := client.Send([][]byte{
	//	[]byte("set"),
	//	[]byte("a"),
	//	[]byte("a"),
	//})
	//
	//if statusRet, ok := result.(*protocol.StatusReply); ok {
	//	log.Println(statusRet.Status)
	//	if statusRet.Status != "OK" {
	//		t.Error("`set` failed, result: " + statusRet.Status)
	//	}
	//} else {
	//	t.Error("assert error")
	//}

	//result := client.Send([][]byte{
	//	[]byte("get"),
	//	[]byte("a"),
	//})
	//
	//t.Logf("%v \n", string(result.ToBytes()))
	//if bulkRet, ok := result.(*protocol.BulkReply); ok {
	//	if string(bulkRet.Arg) != "a" {
	//		t.Error("`get` failed, result: " + string(bulkRet.Arg))
	//	} else {
	//		t.Logf("get successfully!")
	//	}
	//} else {
	//	t.Error("assert error")
	//}
}

func TestZset(t *testing.T) {
	client, err := MakeClusterClient(&Config{
		EtcdAddr:    []string{"127.0.0.1:2379"},
		DialTimeOut: 1000000000,
	})
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result1 := client.Send([][]byte{
		[]byte("zadd"),
		[]byte("key1"),
		[]byte("10"),
		[]byte("mem1"),
	})
	if statusRet, ok := result1.(*protocol.StatusReply); ok {
		log.Println(statusRet.Status)
	} else {
		t.Error("zadd assert")
	}

	result2 := client.Send([][]byte{
		[]byte("zadd"),
		[]byte("key1"),
		[]byte("20"),
		[]byte("mem2"),
	})
	if statusRet, ok := result2.(*protocol.StatusReply); ok {
		log.Println(statusRet.Status)
	} else {
		t.Error("zadd assert")
	}

	reply := client.Send([][]byte{
		[]byte("zrangebylex"),
		[]byte("key1"),
		[]byte("mem"),
		[]byte("z"),
		[]byte("withscore"),
	})

	if rep, ok := reply.(*protocol.MultiBulkStringReply); ok {
		for _, arg := range rep.Args {
			log.Println(string(arg))
		}
	} else {
		t.Error("zrange assert")
	}
}

func TestSplit(t *testing.T) {
	test := "a  b   123"
	for _, s := range strings.Split(test, " ") {
		fmt.Println(s)
	}
}
