package database

import (
	"reids-by-go/datastruct/redis_hash"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/trans"
	"strconv"
)

/*
	HSET：将哈希表中的字段设置为指定值。如果字段已经存在，则覆盖旧值。
	HGET：获取哈希表中指定字段的值。
	HDEL：从哈希表中删除一个或多个字段。
	HEXISTS：检查哈希表中是否存在指定字段。
	HGETALL：获取哈希表中所有字段和值。
	HINCRBY：将哈希表中指定字段的值增加一个整数。
	HINCRBYFLOAT：将哈希表中指定字段的值增加一个浮点数。
	HKEYS：获取哈希表中所有字段的列表。
	HLEN：获取哈希表中字段的数量。
	HMSET：同时设置多个字段的值。
	HMGET：获取多个字段的值。
	HSETNX：只在字段不存在时，将字段设置为指定值。
	HVALS：获取哈希表中所有值的列表。
	HSCAN：迭代遍历哈希表中的字段和值。
*/

func hSetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HSet(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func hGetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HGet(cmdStrings[1], cmdStrings[2])
}

func hDelByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HDel(cmdStrings[1], cmdStrings[2])
}

func hExistsByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HExists(cmdStrings[1], cmdStrings[2])
}

func hGetAllByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HGetAll(cmdStrings[1])
}

func hIncByByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HIncBy(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func hIncByFloatByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HIncByFloat(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func hKeysByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HKeys(cmdStrings[1], cmdStrings[2])
}

func hLenByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HLen(cmdStrings[1], cmdStrings[2])
}

func hMSetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 || len(cmdStrings)%2 != 0 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	var setMap map[string]any
	for i := 2; i < len(cmdStrings); i += 2 {
		setMap[cmdStrings[i]] = cmdStrings[i+1]
	}
	return db.HMSet(cmdStrings[1], setMap)
}

func hMGetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HMGet(cmdStrings[1], cmdStrings[2:])
}

func hSetNxByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HSetNx(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func hValsByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.HVals(cmdStrings[1])
}

func hScanByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 5 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	cursor, err := strconv.Atoi(cmdStrings[2])
	count, err := strconv.Atoi(cmdStrings[4])
	if err != nil {
		return protocol.MakeErrReply("COMMAND'S FORMAT ERROR"), nil
	}
	return db.HScan(cmdStrings[1], cursor, cmdStrings[3], count)
}

func (db *DB) HSet(key string, field string, val any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if b := hash.Set(field, val); b {
			return protocol.MakeStatusReply("OK! THE KEY HAS BEEN SET, KEY:" + key + " VALUE: " + field + " " + trans.AnyToString(val)), nil
		}
		return protocol.MakeStatusReply(""), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HGet(key string, field string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if a, b := hash.Get(field); b {
			return protocol.MakeBulkReply(trans.AnyToBytes(a)), nil
		}
		return protocol.MakeStatusReply("FAILED! THE FIELD DO NOT EXISTED"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HDel(key string, field string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if del := hash.Del(field); del {
			return protocol.MakeStatusReply("OK! THE KEY'S FIELD HAS BEEN DELETED"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE FIELD DO NOT EXISTED"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HExists(key string, field string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if b := hash.Exists(field); b {
			return protocol.MakeStatusReply("OK! THE FIELD IS EXIST"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE FIELD DO NOT EXISTED"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HGetAll(key string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		reply := make([][]byte, 0)
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		all := hash.GetAll()
		for _, anys := range all {
			for _, a := range anys {
				reply = append(reply, trans.AnyToBytes(a))
			}
		}
		return protocol.MakeMultiBulkReply(reply), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HIncBy(key string, field string, inc any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if by := hash.IncBy(field, inc); by {
			return protocol.MakeStatusReply("OK! FIELD INC SUCCESSFULLY!"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE FIELD DO NOT EXISTED OR INC FORMAT ERROR!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HIncByFloat(key string, field string, inc any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if by := hash.IncBy(field, inc); by {
			return protocol.MakeStatusReply("OK! FIELD INC SUCCESSFULLY!"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE FIELD DO NOT EXISTED OR INC FORMAT ERROR!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HKeys(key string, field string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		keys := hash.Keys(field)
		return protocol.MakeMultiBulkReply(trans.StringsToBytes(keys)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HLen(key string, field string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		i := hash.Len(field)
		return protocol.MakeIntReply(int64(i)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HMSet(key string, s map[string]any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		mSet := hash.MSet(s)
		return protocol.MakeStatusReply("OK! HAVE BEEN SET," + strconv.Itoa(mSet)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HMGet(key string, field []string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		mGet := hash.MGet(field)
		return protocol.MakeMultiBulkReply(trans.AnysToBytes(mGet)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HSetNx(key string, filed string, val any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		if nx := hash.SetNX(filed, val); nx {
			return protocol.MakeStatusReply("OK! THE KV HAVE BEEN SET!"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE KEY MAY HAVE EXIST"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HVals(key string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		vals := hash.Vals()
		return protocol.MakeMultiBulkReply(trans.AnysToBytes(vals)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) HScan(key string, cursor int, match string, count int) (redis.Reply, *Extra) {
	//HSCAN key cursor [MATCH pattern] [COUNT count]
	value, exists := db.data.Get(key)
	if exists {
		hash, ok := value.(*redis_hash.Hash)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT HASH!"), nil
		}
		vals := hash.Scan(cursor, match, count)
		return protocol.MakeMultiBulkReply(trans.MapToBytes(vals)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}
