package database

import (
	"bytes"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"strings"
)

func (db *DB) Exec(cmd CmdLine) redis.Reply {
	var cmdStrings []string
	for _, bytesCmd := range cmd {
		cmdStrings = append(cmdStrings, string(bytes.TrimSuffix(bytesCmd, []byte{'\r', '\n'})))
		//log.Print("exec >>>> ", string(bytes))
	}
	switch strings.ToLower(cmdStrings[0]) {
	case "set":
		return db.Set(cmdStrings[1], cmdStrings[2])
	case "get":
		return db.Get(cmdStrings[1])
	case "delete":
		return db.Delete(cmdStrings[1])
	case "ping":
		return db.Ping()
	}
	return nil
}

func (db *DB) Ping() redis.Reply {
	return protocol.MakeBulkReply([]byte("pong"))
}

func (db *DB) Set(key string, value any) redis.Reply {
	//log.Println("exec >>> set", key, value)
	result := db.data.Put(key, value)
	//todo aof
	if result == 1 {
		return protocol.MakeStatusReply("SET OK")
	} else {
		return protocol.MakeStatusReply("SET FAILED")
	}
}

func (db *DB) Get(key string) redis.Reply {
	val, exists := db.data.Get(key)
	//todo aof

	if exists {
		return protocol.MakeBulkReply([]byte(val.(string)))
	} else {
		return protocol.MakeBulkReply([]byte("have existed! key:" + key))
	}
}

func (db *DB) Delete(key string) redis.Reply {
	result := db.data.Delete(key)
	//todo aof

	if result == 1 {
		return protocol.MakeStatusReply("DELETE OK")
	} else {
		return protocol.MakeStatusReply("THE KEY IS NOT EXISTED")
	}
}
