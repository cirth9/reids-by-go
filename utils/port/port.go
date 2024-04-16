package port

import (
	"fmt"
	"net"
	"reids-by-go/config"
	"strconv"
	"time"
)

func GetFreePort() string {
	if config.IsDev {
		return "9999"
	}
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		fmt.Println("Failed to listen:", err)
		return ""
	}
	// 获取实际监听的地址
	address := listener.Addr().(*net.TCPAddr)
	freePort := strconv.Itoa(address.Port)
	listener.Close()
	time.Sleep(time.Millisecond * 500)
	return freePort
}
