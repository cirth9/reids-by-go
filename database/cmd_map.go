package database

import (
	"reids-by-go/interface/redis"
)

// common,对key进行直接操作，忽略背后的数据结构
const (
	set       = "set"
	get       = "get"
	mSet      = "mset"
	mGet      = "mget"
	deleteKey = "delete"
	ping      = "ping"
)

// zSet
const (
	zAdd        = "zadd"
	zRem        = "zrem"
	zRange      = "zrange"
	zRangeByLex = "zrangebylex"
	zRank       = "zrank"
	zScore      = "zscore"
	zCount      = "zcount"
	zIncBy      = "zincby"
	zCard       = "zcard"
)

// Expire
const (
	expire    = "expire"
	pExpire   = "pexpire"
	expireAt  = "expireat"
	pExpireAt = "pexpireat"
	ttl       = "ttl"
	pTTL      = "pttl"
	persist   = "persist"
)

// Hash
const (
	hSet         = "hset"
	hGet         = "hget"
	hDel         = "hdel"
	hExists      = "hexists"
	hGetAll      = "hgetall"
	hIncrBy      = "hincrby"
	hIncrByFloat = "hincrbyfloat"
	hKeys        = "hkeys"
	hLen         = "hlen"
	hMSet        = "hmset"
	hMGet        = "hmget"
	hSetNx       = "hsetnx"
	hVals        = "hvals"
	hScan        = "hscan"
)

// redis_list
const (
	lPush   = "lpush"
	rPush   = "rpush"
	lPop    = "lpop"
	rPop    = "rpop"
	lIndex  = "lindex"
	lLen    = "llen"
	lRange  = "lrange"
	lInsert = "linser"
	lSet    = "lset"
	lRem    = "lrem"
	lTrim   = "ltrim"
)

// set
const (
	sAdd         = "sadd"
	sRem         = "srem"
	sIsMember    = "sismember"
	sCard        = "scard"
	sMembers     = "smembers"
	sRangeMember = "srangemember"
	sPop         = "spop"
	sInter       = "sinter"
	sUnion       = "sunion"
	sDiff        = "sdiff"
	sInterStore  = "sinterstore"
	sUnionStore  = "sunionstore"
	sDiffStore   = "sdiffstore"
	sMove        = "smove"
	sRandMember  = "srandmember"
)

type command struct {
	cmd     string
	handler func(cmdStrings []string, db *DB) (redis.Reply, *Extra)
}

func makeCommand(cmd string, handler func(cmdStrings []string, db *DB) (redis.Reply, *Extra)) *command {
	return &command{
		cmd:     cmd,
		handler: handler,
	}
}

var cmdMap = initCmdMap()

func initCmdMap() map[string]*command {
	cmd := make(map[string]*command)

	//todo common
	cmd[get] = makeCommand(get, getByDb)
	cmd[deleteKey] = makeCommand(deleteKey, deleteByDb)
	cmd[set] = makeCommand(set, setByDb)
	cmd[ping] = makeCommand(ping, pingByDb)
	cmd[mSet] = makeCommand(mSet, mSetByDb)
	cmd[mGet] = makeCommand(mGet, mGetByDb)

	//todo zSet
	cmd[zAdd] = makeCommand(zAdd, zAddByDb)                      //
	cmd[zRem] = makeCommand(zRem, zRemByDb)                      //
	cmd[zRange] = makeCommand(zRange, zRangeByDb)                //
	cmd[zRangeByLex] = makeCommand(zRangeByLex, zRangeByLexByDb) //
	cmd[zRank] = makeCommand(zRank, zRankByDb)
	cmd[zScore] = makeCommand(zScore, zScoreByDb)
	cmd[zCount] = makeCommand(zCount, zCountByDb) //
	cmd[zIncBy] = makeCommand(zIncBy, zIncByDb)
	cmd[zCard] = makeCommand(zCard, zCardByDb)

	//todo expire
	cmd[expire] = makeCommand(expire, expireByDb)
	cmd[pExpire] = makeCommand(pExpire, pExpireByDb)
	cmd[expireAt] = makeCommand(expireAt, expireAtByDb)
	cmd[pExpireAt] = makeCommand(pExpireAt, pExpireAtByDb)
	cmd[ttl] = makeCommand(ttl, ttlByDb)
	cmd[pTTL] = makeCommand(pTTL, pTtlByDb)
	cmd[persist] = makeCommand(persist, persistByDb)

	//todo redis_list
	cmd[lPush] = makeCommand(lPush, lPushByDb)
	cmd[rPush] = makeCommand(rPush, rPushByDb)
	cmd[lPop] = makeCommand(lPop, lPopByDb)
	cmd[rPop] = makeCommand(rPop, rPopByDb)
	cmd[lIndex] = makeCommand(lIndex, lIndexByDb)
	cmd[lLen] = makeCommand(lLen, lLenByDb)
	cmd[lRange] = makeCommand(lRange, lRangeByDb)
	cmd[lInsert] = makeCommand(lInsert, lInsertByDb)
	cmd[lSet] = makeCommand(lSet, lSetByDb)
	cmd[lRem] = makeCommand(lRem, lRemByDb)
	cmd[lTrim] = makeCommand(lTrim, lTrimByDb)

	//todo set
	cmd[sAdd] = makeCommand(sAdd, sAddByDb)
	cmd[sRem] = makeCommand(sRem, sRemByDb)
	cmd[sIsMember] = makeCommand(sIsMember, sisMemberByDb)
	cmd[sCard] = makeCommand(sCard, sCardByDb)
	cmd[sMembers] = makeCommand(sMembers, sMembersByDb)
	cmd[sRangeMember] = makeCommand(sRangeMember, sRangeMemberByDb)
	cmd[sPop] = makeCommand(sPop, sPopByDb)
	cmd[sInter] = makeCommand(sInter, sInterByDb)
	cmd[sUnion] = makeCommand(sUnion, sUnionByDb)
	cmd[sDiff] = makeCommand(sDiff, sDiffByDb)
	cmd[sInterStore] = makeCommand(sInterStore, sInterStoreByDb)
	cmd[sUnionStore] = makeCommand(sUnionStore, sUnionStoreByDb)
	cmd[sDiffStore] = makeCommand(sDiffStore, sDiffStoreByDb)
	cmd[sMove] = makeCommand(sMove, sMoveByDb)
	cmd[sRandMember] = makeCommand(sRandMember, sRandMemberByDb)

	//todo hash
	cmd[hSet] = makeCommand(hSet, hSetByDb)
	cmd[hGet] = makeCommand(hGet, hGetByDb)
	cmd[hDel] = makeCommand(hDel, hDelByDb)
	cmd[hExists] = makeCommand(hExists, hExistsByDb)
	cmd[hGetAll] = makeCommand(hGetAll, hGetAllByDb)
	cmd[hIncrBy] = makeCommand(hIncrBy, hIncByByDb)
	cmd[hIncrByFloat] = makeCommand(hIncrByFloat, hIncByFloatByDb)
	cmd[hKeys] = makeCommand(hKeys, hKeysByDb)
	cmd[hLen] = makeCommand(hLen, hLenByDb)
	cmd[hMSet] = makeCommand(hMSet, hMSetByDb)
	cmd[hMGet] = makeCommand(hMGet, hMGetByDb)
	cmd[hSetNx] = makeCommand(hSetNx, hSetNxByDb)
	cmd[hVals] = makeCommand(hVals, hValsByDb)
	cmd[hScan] = makeCommand(hScan, hScanByDb)
	return cmd
}
