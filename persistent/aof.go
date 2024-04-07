package persistent

import (
	"os"
	"reids-by-go/redis/protocol"
	"sync"
)

type Persist struct {
	aofChan        chan *protocol.MultiBulkStringReply
	aofFile        *os.File
	aofFileName    string
	aofRewriteChan chan *protocol.MultiBulkStringReply
	pausingAof     sync.RWMutex
}

type extra struct {
	toPersist  bool
	specialAof []*protocol.MultiBulkStringReply
}

type Handler struct {
}

func (h *Handler) StartRewrite() {

}
