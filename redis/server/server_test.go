package server

import (
	"reids-by-go/tcp"
	"testing"
)

func TestServer(t *testing.T) {
	err := tcp.ListenAndServerWithSignal(&tcp.Config{
		Address: ":9999",
	}, NewHandler())
	if err != nil {
		panic(err)
	}
}
