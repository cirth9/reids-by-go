package database

import (
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"strconv"
	"time"
)

func getExpireTask(key string) string {
	return "expire:" + key
}

func CheckExpire() {

}

//expireTime unix统一使用毫秒级时间戳存储

func expireByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.Expire(cmdStrings)
}

func pExpireByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.PExpire(cmdStrings)
}

func expireAtByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.ExpireAt(cmdStrings)
}

func pExpireAtByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.PExpireAt(cmdStrings)
}

func ttlByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.TTL(cmdStrings)
}

func pTtlByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.PTTL(cmdStrings)
}

func persistByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.Persist(cmdStrings)
}

func (db *DB) Expire(cmdStrings []string) (redis.Reply, *Extra) {
	//expire key secondsUnix
	expireTime, err := strconv.ParseInt(cmdStrings[2], 10, 64)
	if err != nil {
		return protocol.MakeErrReply("EXPIRE PARAMS FORMAT ERROR:" + err.Error()), nil
	}
	return db.expire(cmdStrings[1], expireTime)
}

func (db *DB) PExpire(cmdStrings []string) (redis.Reply, *Extra) {
	//pexpire key millisecondsUnix
	pexpireTime, err := strconv.ParseInt(cmdStrings[2], 10, 64)
	if err != nil {
		return protocol.MakeErrReply("EXPIRE PARAMS FORMAT ERROR:" + err.Error()), nil
	}
	return db.pExpire(cmdStrings[1], pexpireTime)
}

func (db *DB) ExpireAt(cmdStrings []string) (redis.Reply, *Extra) {
	//expireAt key secondsUnix
	expireTime, err := strconv.ParseInt(cmdStrings[2], 10, 64)
	if err != nil {
		return protocol.MakeErrReply("EXPIRE PARAMS FORMAT ERROR:" + err.Error()), nil
	}
	return db.expireAt(cmdStrings[1], expireTime)
}

func (db *DB) PExpireAt(cmdStrings []string) (redis.Reply, *Extra) {
	//pexpireAt key millisecondsUnix
	pexpireTime, err := strconv.ParseInt(cmdStrings[2], 10, 64)
	if err != nil {
		return protocol.MakeErrReply("EXPIRE PARAMS FORMAT ERROR:" + err.Error()), nil
	}
	return db.pExpireAt(cmdStrings[1], pexpireTime)
}

func (db *DB) TTL(cmdStrings []string) (redis.Reply, *Extra) {
	return db.ttl(cmdStrings[1])
}

func (db *DB) PTTL(cmdStrings []string) (redis.Reply, *Extra) {
	return db.pTTL(cmdStrings[1])
}

func (db *DB) Persist(cmdStrings []string) (redis.Reply, *Extra) {
	return db.persist(cmdStrings[1])
}

func (db *DB) expire(key string, seconds int64) (redis.Reply, *Extra) {
	//log.Println("exec >>> set", key, value)
	expireTime := time.Now().UnixMilli() + time.Second.Milliseconds()*seconds
	result := db.ttlMap.Put(key, expireTime)
	//todo persist
	if result == 1 {
		return protocol.MakeStatusReply("EXPIRE SET OK \nKEY:" + key + "\nEXPIRE TIME:" + time.UnixMilli(expireTime).String()), nil
	} else {
		return protocol.MakeStatusReply("EXPIRE TIME SET FAILED"), nil
	}
}

func (db *DB) pExpire(key string, milliseconds int64) (redis.Reply, *Extra) {
	expireTime := time.Now().UnixMilli() + time.Millisecond.Milliseconds()*milliseconds
	result := db.ttlMap.Put(key, expireTime)
	//todo persist
	if result == 1 {
		return protocol.MakeStatusReply("EXPIRE SET OK \nKEY:" + key + "\nEXPIRE TIME:" + time.UnixMilli(expireTime).String()), nil
	} else {
		return protocol.MakeStatusReply("EXPIRE TIME SET FAILED"), nil
	}
}

func (db *DB) expireAt(key string, unixSecondTime int64) (redis.Reply, *Extra) {
	expireTime := time.Unix(unixSecondTime, 0).UnixMilli()
	result := db.ttlMap.Put(key, expireTime)
	//todo persist
	if result == 1 {
		return protocol.MakeStatusReply("EXPIRE SET OK \nKEY:" + key + "\nEXPIRE TIME:" + time.UnixMilli(expireTime).String()), nil
	} else {
		return protocol.MakeStatusReply("EXPIRE TIME SET FAILED"), nil
	}
}

func (db *DB) pExpireAt(key string, unixMilliSecondsTime int64) (redis.Reply, *Extra) {
	expireTime := unixMilliSecondsTime
	result := db.ttlMap.Put(key, expireTime)
	//todo persist
	if result == 1 {
		return protocol.MakeStatusReply("EXPIRE SET OK \nKEY:" + key + "\nEXPIRE TIME:" + time.UnixMilli(expireTime).String()), nil
	} else {
		return protocol.MakeStatusReply("EXPIRE TIME SET FAILED"), nil
	}
}

func (db *DB) ttl(key string) (redis.Reply, *Extra) {
	//seconds
	val, exists := db.ttlMap.Get(key)
	if exists {
		ttl := time.UnixMilli(val.(int64)).Unix() - time.Now().Unix()
		if ttl > 0 {
			return protocol.MakeTimeReply(time.Unix(ttl, 0)), nil
		}
	}
	return protocol.MakeStatusReply("NO SUCH EXPIRE KEY"), nil
}

func (db *DB) pTTL(key string) (redis.Reply, *Extra) {
	//milliseconds
	val, exists := db.ttlMap.Get(key)
	if exists {
		ttl := val.(int64) - time.Now().UnixMilli()
		if ttl > 0 {
			return protocol.MakeTimeReply(time.UnixMilli(ttl)), nil
		}
	}
	return protocol.MakeStatusReply("NO SUCH EXPIRE KEY"), nil
}

func (db *DB) persist(key string) (redis.Reply, *Extra) {
	i := db.ttlMap.Delete(key)
	if i == 1 {
		return protocol.MakeStatusReply("OK! DELETE EXPIRE TIME SUCCESSFULLY! KEY:" + key), nil
	}
	return protocol.MakeStatusReply("NO SUCH EXPIRE KEY"), nil
}

func expireRollBack(cmdLine CmdLine, db *DB) CmdLine {
	//expire key secondsUnix
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte("persist"))
	rollback = append(rollback, cmdLine[1])
	return rollback
}

func pExpireRollBack(cmdLine CmdLine, db *DB) CmdLine {
	//expire key secondsUnix
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte("persist"))
	rollback = append(rollback, cmdLine[1])
	return rollback
}

func persistRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte("pexpire"))
	val, exists := db.ttlMap.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, []byte(strconv.FormatInt(val.(int64), 10)))
	return rollback
}
