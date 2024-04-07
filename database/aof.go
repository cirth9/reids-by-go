package database

import (
	"errors"
	"io"
	"log"
	"os"
	"reids-by-go/config"
	"reids-by-go/redis/parse"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/cmd_utils"
	"reids-by-go/utils/trans"
	"strings"
	"time"
)

type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
}

func (db *DB) handleAof() {
	for p := range db.aofChan {
		db.pausingAof.RLock()
		data := protocol.MakeMultiBulkReply(p.Args).ToBytes()
		//log.Println(string(data))
		_, err := db.aofFile.Write(data)
		if err != nil {
			log.Println("aof file write error:", err.Error())
		}
		db.pausingAof.RUnlock()
	}
	db.aofFinished <- struct{}{}
}

func (db *DB) loadAof(maxBytes int64) {
	aofChan := db.aofChan
	db.aofChan = nil
	defer func(aofChan chan *protocol.MultiBulkStringReply) {
		db.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(db.aofFilename)
	if err != nil {
		var pathError *os.PathError
		if errors.As(err, &pathError) {
			return
		}
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println(err)
		}
	}(file)

	reader := io.LimitReader(file, maxBytes)
	ch := parse.ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			log.Println("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			log.Println("empty payload")
			continue
		}
		log.Println("load Aof:", string(p.Data.ToBytes()))
		r, ok := p.Data.(*protocol.MultiBulkStringReply)
		if !ok {
			log.Println("require multi bulk reply")
			continue
		}
		cmd := strings.ToLower(string(r.Args[0]))
		c, ok := cmdMap[cmd]
		//log.Println("load Aof cmd", cmd)
		if ok {
			cmdStrings := trans.BytesToStrings(r.Args)
			c.handler(cmdStrings, db)
		}
	}
}

func (db *DB) AofReWrite() {
	for {
		time.Sleep(time.Second * time.Duration(config.PersistConfig.AofRewriteTime))
		//log.Println("AOF Rewrite Start")
		rewrite, err := db.StartRewrite()
		if err != nil {
			log.Panic("aof rewrite error")
		}
		err = db.DoRewrite(rewrite)
		if err != nil {
			log.Panic("rewrite Failed")
		}
		db.FinishedRewrite(rewrite)
	}
}

func (db *DB) StartRewrite() (*RewriteCtx, error) {
	db.pausingAof.Lock()
	defer db.pausingAof.Unlock()

	err := db.aofFile.Sync()
	if err != nil {
		log.Println("aof start rewrite sync error:", err.Error())
		return nil, err
	}

	fileInfo, _ := os.Stat(db.aofFilename)
	fileSize := fileInfo.Size()

	file, err := os.CreateTemp(config.PersistConfig.TmpFile, "*.aof")
	if err != nil {
		log.Println("aof start rewrite error:", err.Error())
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: fileSize,
	}, nil
}

func (db *DB) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile
	db.loadAof(ctx.fileSize)
	db.ForEach(func(key string, data any, expiration *time.Time) bool {
		cmd := cmd_utils.DataToCmd(key, data)
		if cmd != nil {
			tmpFile.Write(cmd.ToBytes())
		}
		if expiration != nil {
			expireCmd := cmd_utils.MakeExpireCmd(key, *expiration)
			if cmd != nil {
				tmpFile.Write(expireCmd.ToBytes())
			}
		}
		return true
	})

	return nil
}

func (db *DB) FinishedRewrite(ctx *RewriteCtx) {
	db.pausingAof.Lock()
	defer db.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	src, err := os.Open(db.aofFilename)
	if err != nil {
		log.Println("open aof file failed", err.Error())
		return
	}
	defer src.Close()

	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		log.Println("aof finish rewrite seek error:", err.Error())
		return
	}

	_, err = io.Copy(tmpFile, src)
	if err != nil {
		log.Println("copy aof file failed:" + err.Error())
		return
	}
	db.aofFile.Close()

	os.Rename(tmpFile.Name(), db.aofFilename)

	var aofFile *os.File
	aofFile, err = os.OpenFile(db.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	db.aofFile = aofFile
	return
}
