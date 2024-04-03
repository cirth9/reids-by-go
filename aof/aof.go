package aof

import (
	"context"
	"os"
	"sync"
)

type CmdLine = [][]byte

type payload struct {
	cmdline CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

type Persist struct {
	ctx         context.Context
	cancel      context.CancelFunc
	aofChan     chan *payload
	aofFile     *os.File
	aofFileName string
	aofFsync    string
	aofFinished chan struct{}
	pausingAof  sync.Mutex
	currentDB   int

	buffer []CmdLine
}
