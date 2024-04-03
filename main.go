package main

import (
	"reids-by-go/redis/server"
	"reids-by-go/tcp"
)

func main() {
	err := tcp.ListenAndServerWithSignal(&tcp.Config{
		Address: ":9999",
	}, server.NewHandler())
	if err != nil {
		panic(err)
	}
}
