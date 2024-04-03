package client

import (
	"log"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"testing"
)

type Test func(redis.Reply)

func TestClient(t *testing.T) {
	client, err := MakeClient("localhost:9999")
	if err != nil {
		t.Error(err)
	}
	client.Start()
	result := client.Send([][]byte{
		[]byte("set"),
		[]byte("a"),
		[]byte("a"),
	})

	if statusRet, ok := result.(*protocol.StatusReply); ok {
		log.Println(statusRet.Status)
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	} else {
		t.Error("assert error")
	}

	result = client.Send([][]byte{
		[]byte("get"),
		[]byte("a"),
	})

	t.Logf("%v \n", string(result.ToBytes()))
	if bulkRet, ok := result.(*protocol.BulkReply); ok {
		if string(bulkRet.Arg) != "a" {
			t.Error("`get` failed, result: " + string(bulkRet.Arg))
		} else {
			t.Logf("get successfully!")
		}
	} else {
		t.Error("assert error")
	}
}
