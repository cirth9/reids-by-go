package database

import (
	"log"
	"os"
	"reids-by-go/config"
	"reids-by-go/datastruct/dict"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/trans"
	"strings"
	"sync"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

type DB struct {
	//index int

	// key -> D
	//ataEntity
	data *dict.ConcurrentDict

	// key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict

	// key -> version(uint32)
	versionMap *dict.ConcurrentDict

	//// callbacks
	//insertCallback database.KeyEventCallback
	//deleteCallback database.KeyEventCallback

	//todo   about persist
	// 主线程使用此channel将要持久化的命令发送到异步协程
	aofChan        chan *protocol.MultiBulkStringReply
	aofFile        *os.File
	aofFilename    string
	aofRewriteChan chan *protocol.MultiBulkStringReply
	pausingAof     sync.RWMutex
	aofFinished    chan struct{}
	addAof         func(CmdLine)
}

type Extra struct {
	toPersist  bool
	specialAof []*protocol.MultiBulkStringReply
}

type CmdLine [][]byte

func NewDatabase() *DB {
	var err error
	db := &DB{
		data:           dict.MakeConcurrent(dataDictSize),
		ttlMap:         dict.MakeConcurrent(ttlDictSize),
		versionMap:     dict.MakeConcurrent(dataDictSize),
		aofChan:        make(chan *protocol.MultiBulkStringReply),
		aofRewriteChan: make(chan *protocol.MultiBulkStringReply),
		pausingAof:     sync.RWMutex{},
		aofFinished:    make(chan struct{}),
		addAof:         func(line CmdLine) {},
	}
	db.aofFile, err = os.OpenFile(config.PersistConfig.AofFile, os.O_APPEND|os.O_RDWR, 7777)
	if err != nil {
		panic("aof file get error!" + err.Error())
	}
	db.aofFilename = db.aofFile.Name()
	log.Println("AOF FileName:", db.aofFile.Name())
	db.loadAof(1024)
	go db.handleAof()
	go db.AofReWrite()
	return db
}

func (db *DB) RoundDeleteExpiredKey(checkTime time.Duration) {
	go func() {
		for {
			time.Sleep(checkTime)
			db.ttlMap.ForEach(func(key string, val any) bool {
				expireTime := val.(int64)
				if expireTime < time.Now().UnixMilli() {
					db.ttlMap.Delete(key)
				}
				return true
			})
		}
	}()
}
func (db *DB) RWUnLock(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

func (db *DB) RWLock(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

// Exec  todo 解析redis命令
func (db *DB) Exec(cmd CmdLine) redis.Reply {
	cmdStrings := trans.BytesToStrings(cmd)
	//var extra *Extra
	cmdHeader := strings.ToLower(cmdStrings[0])
	c, ok := cmdMap[cmdHeader]
	if !ok {
		return protocol.MakeErrReply("NO SUCH COMMAND")
	}
	reply, extra := c.handler(cmdStrings, db)
	if extra != nil && extra.toPersist {
		go func() {
			for _, stringReply := range extra.specialAof {
				db.aofChan <- stringReply
			}
		}()
	}
	//log.Println(string(reply.ToBytes()))
	return reply
}

func (db *DB) ForEach(h func(key string, data any, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val any) bool {
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(int64)
			unixMilli := time.UnixMilli(expireTime)
			expiration = &unixMilli
		}
		return h(key, val, expiration)
	})
}

func (db *DB) GetUndoLogs(lines []CmdLine) []CmdLine {
	undoLog := make([]CmdLine, 0)
	for i := 0; i < len(lines); i++ {
		undoCmdFunc := CmdUnDoMap[string(lines[i][0])]
		undoCmd := undoCmdFunc(lines[i], db)
		undoLog = append(undoLog, undoCmd)
	}
	return undoLog
}

func (db *DB) CheckExpire() {

}
