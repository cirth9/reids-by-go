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
	hSet        = "hset"
	hGet        = "hget"
	hDel        = "hdel"
	hExists     = "hexists"
	hGetAll     = "hgetall"
	hIncBy      = "hincrby"
	hIncByFloat = "hincrbyfloat"
	hKeys       = "hkeys"
	hLen        = "hlen"
	hMSet       = "hmset"
	hMGet       = "hmget"
	hSetNx      = "hsetnx"
	hVals       = "hvals"
	hScan       = "hscan"
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

type CmdHandlerByDb func(cmdStrings []string, db *DB) (redis.Reply, *Extra)

type command struct {
	cmd     string
	handler CmdHandlerByDb
}

func makeCommand(cmd string, handler CmdHandlerByDb) *command {
	return &command{
		cmd:     cmd,
		handler: handler,
	}
}

var cmdMap = initCmdMap()
var CmdIsWriteMap = initCmdIsWriteMap()
var CmdUnDoMap = initCmdUnDoMap()

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
	cmd[sPop] = makeCommand(sPop, sPopByDb)
	cmd[sInter] = makeCommand(sInter, sInterByDb)
	cmd[sUnion] = makeCommand(sUnion, sUnionByDb)
	cmd[sDiff] = makeCommand(sDiff, sDiffByDb)
	cmd[sRandMember] = makeCommand(sRandMember, sRandMemberByDb)
	//cmd[sInterStore] = makeCommand(sInterStore, sInterStoreByDb)
	//cmd[sUnionStore] = makeCommand(sUnionStore, sUnionStoreByDb)
	//cmd[sDiffStore] = makeCommand(sDiffStore, sDiffStoreByDb)
	//cmd[sMove] = makeCommand(sMove, sMoveByDb)

	//todo hash
	cmd[hSet] = makeCommand(hSet, hSetByDb)
	cmd[hGet] = makeCommand(hGet, hGetByDb)
	cmd[hDel] = makeCommand(hDel, hDelByDb)
	cmd[hExists] = makeCommand(hExists, hExistsByDb)
	cmd[hGetAll] = makeCommand(hGetAll, hGetAllByDb)
	cmd[hIncBy] = makeCommand(hIncBy, hIncByByDb)
	cmd[hIncByFloat] = makeCommand(hIncByFloat, hIncByFloatByDb)
	cmd[hKeys] = makeCommand(hKeys, hKeysByDb)
	cmd[hLen] = makeCommand(hLen, hLenByDb)
	cmd[hMSet] = makeCommand(hMSet, hMSetByDb)
	cmd[hMGet] = makeCommand(hMGet, hMGetByDb)
	cmd[hSetNx] = makeCommand(hSetNx, hSetNxByDb)
	cmd[hVals] = makeCommand(hVals, hValsByDb)
	cmd[hScan] = makeCommand(hScan, hScanByDb)
	return cmd
}

func initCmdIsWriteMap() map[string]bool {
	cmd := make(map[string]bool)

	//todo common
	cmd[get] = false
	cmd[deleteKey] = true
	cmd[set] = true
	cmd[ping] = false
	cmd[mSet] = true
	cmd[mGet] = false

	//todo zSet
	cmd[zAdd] = true
	cmd[zRem] = true
	cmd[zRange] = false //
	cmd[zRangeByLex] = false
	cmd[zRank] = false
	cmd[zScore] = false
	cmd[zCount] = false
	cmd[zIncBy] = true
	cmd[zCard] = false

	//todo expire
	cmd[expire] = true
	cmd[pExpire] = true
	cmd[expireAt] = false
	cmd[pExpireAt] = false
	cmd[ttl] = false
	cmd[pTTL] = false
	cmd[persist] = true

	//todo redis_list
	cmd[lPush] = true
	cmd[rPush] = true
	cmd[lPop] = true
	cmd[rPop] = true
	cmd[lIndex] = false
	cmd[lLen] = false
	cmd[lRange] = false
	cmd[lInsert] = true
	cmd[lSet] = true
	cmd[lRem] = true
	cmd[lTrim] = true

	//todo set
	cmd[sAdd] = true
	cmd[sRem] = true
	cmd[sIsMember] = false
	cmd[sCard] = false
	cmd[sMembers] = false
	cmd[sRangeMember] = false
	cmd[sPop] = true
	cmd[sInter] = false
	cmd[sUnion] = false
	cmd[sDiff] = false
	cmd[sInterStore] = false
	cmd[sUnionStore] = false
	cmd[sDiffStore] = false
	cmd[sMove] = true
	cmd[sRandMember] = false

	//todo hash
	cmd[hSet] = true
	cmd[hGet] = false
	cmd[hDel] = true
	cmd[hExists] = false
	cmd[hGetAll] = false
	cmd[hIncBy] = true
	cmd[hIncByFloat] = true
	cmd[hKeys] = false
	cmd[hLen] = true
	cmd[hMSet] = true
	cmd[hMGet] = false
	cmd[hSetNx] = true
	cmd[hVals] = false
	cmd[hScan] = false
	return cmd
}

type GetRollBackCmd func(cmdLine CmdLine, db *DB) CmdLine

func initCmdUnDoMap() map[string]GetRollBackCmd {
	cmd := make(map[string]GetRollBackCmd)
	//todo common
	cmd[deleteKey] = deleteRollBack
	cmd[set] = setRollBack
	cmd[mSet] = nil

	//todo zSet
	cmd[zAdd] = zAddRollBack
	cmd[zRem] = zRemRollBack
	cmd[zIncBy] = zIncByRollBack

	//todo expire
	cmd[expire] = expireRollBack
	cmd[pExpire] = pExpireRollBack
	cmd[persist] = persistRollBack

	//todo redis_list
	cmd[lPush] = lPushRollBack
	cmd[rPush] = rPushRollBack
	cmd[lPop] = lPopRollBack
	cmd[rPop] = rPopRollBack
	cmd[lInsert] = lInsertRollBack
	cmd[lSet] = lSetRollBack
	cmd[lRem] = lRemRollBack
	cmd[lTrim] = lTrimRollBack

	////todo set
	//cmd[sAdd] = sAddRollBack
	//cmd[sRem] = sRemRollBack
	//cmd[sPop] = sPopRollBack
	//cmd[sMove] = sMoveRollBack

	//todo hash
	cmd[hSet] = hSetRollBack
	cmd[hDel] = hDelRollBack
	cmd[hIncBy] = hIncByRollBack
	cmd[hIncByFloat] = hIncByFloatRollBack
	cmd[hMSet] = hMSetRollBack
	cmd[hSetNx] = hSetNxRollBack

	return cmd
}
