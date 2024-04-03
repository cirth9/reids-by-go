package server

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"reids-by-go/database"
	"reids-by-go/redis/parse"
	"reids-by-go/redis/protocol"
	"sync"
	"sync/atomic"
)

type Handler struct {
	m      sync.Map
	db     database.DB
	closed atomic.Bool

	//todo aof

	// 主线程使用此channel将要持久化的命令发送到异步协程
	aofChan chan *protocol.MultiBulkStringReply
	// append file 文件描述符
	aofFile *os.File
	// append file 路径
	aofFilename string
	// aof 重写需要的缓冲区，将在AOF重写一节详细介绍
	aofRewriteChan chan *protocol.MultiBulkStringReply
	// 在必要的时候使用此字段暂停持久化操作
	pausingAof sync.RWMutex
}

type extra struct {
	toPersist  bool
	specialAof []*protocol.MultiBulkStringReply
}

func NewHandler() *Handler {
	return &Handler{
		m:      sync.Map{},
		db:     database.NewDatabase(),
		closed: atomic.Bool{},
	}
}

func (handler *Handler) Handle(ctx context.Context, conn net.Conn) {
	payloads := parse.ParseStream(conn)
	for payload := range payloads {
		if payload.Err != nil {
			if payload.Err != io.EOF || !errors.Is(payload.Err, io.ErrUnexpectedEOF) {
				//todo handle error

				return
			}
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := conn.Write(errReply.ToBytes())
			if err != nil {
				//todo handle error

				return
			}
			continue
		}
		if payload.Data == nil {
			log.Println("payload is nil")
			continue
		}

		r, ok := payload.Data.(*protocol.MultiBulkStringReply)
		if !ok {
			log.Println(errors.New("require multi bulk protocol"))
			continue
		}
		//log.Println("cmd lines: ")
		//for _, arg := range r.Args {
		//	log.Print(string(arg))
		//}
		result := handler.db.Exec(r.Args)
		//log.Println("result >>> ", string(result.ToBytes()))
		if result != nil {
			_, _ = conn.Write(result.ToBytes())
		} else {
			_, _ = conn.Write([]byte(protocol.UnKnowBytesResult))
		}
	}
}

func (handler *Handler) Close() error {
	handler.closed.Store(true)
	return nil
}
