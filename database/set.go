package database

import (
	redisSet "reids-by-go/datastruct/set"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/trans"
	"strconv"
)

/*
	SADD key member [member ...]: 向集合中添加一个或多个成员。
	SREM key member [member ...]: 从集合中移除一个或多个成员。
	SISMEMBER key member: 检查指定成员是否存在于集合中。
	SCARD key: 返回集合中的成员数量（基数）。
	SMEMBERS key: 返回集合中的所有成员。
	SRANDMEMBER key [count]: 随机返回集合中的一个或多个成员。
	SPOP key [count]: 随机移除并返回集合中的一个或多个成员。
	SINTER key [key ...]: 返回多个集合的交集。
	SUNION key [key ...]: 返回多个集合的并集。
	SDIFF key [key ...]: 返回第一个集合与其他集合的差集。
	SINTERSTORE destination key [key ...]: 将多个集合的交集存储到一个新的集合中。
	SUNIONSTORE destination key [key ...]: 将多个集合的并集存储到一个新的集合中。
	SDIFFSTORE destination key [key ...]: 将第一个集合与其他集合的差集存储到一个新的集合中。
	SMOVE source destination member: 将指定成员从一个集合移动到另一个集合。
	SRANDMEMBER key [count]: 随机返回集合中的一个或多个成员，可重复。
*/

func sAddByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SAdd(cmdStrings[1], cmdStrings[2:])
}

func sRemByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SRem(cmdStrings[1], cmdStrings[2:])
}

func sisMemberByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SisMember(cmdStrings[1], cmdStrings[2])
}

func sPopByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	count, err := strconv.Atoi(cmdStrings[2])
	if err != nil {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SPop(cmdStrings[1], count)
}

func sInterByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SInter(cmdStrings[1:])
}

func sUnionByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SUnion(cmdStrings[1:])
}

func sDiffByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SDiff(cmdStrings[1:])
}

func sCardByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SCard(cmdStrings[1])
}

func sMembersByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SMembers(cmdStrings[1])
}

func sRandMemberByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	count, err := strconv.Atoi(cmdStrings[2])
	if err != nil {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.SRandMember(cmdStrings[1], count)
}

//func sInterStoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
//	if len(cmdStrings) < 2 {
//		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
//	}
//	return db.SInterStore(cmdStrings[1:])
//}
//
//func sUnionStoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
//	if len(cmdStrings) < 2 {
//		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
//	}
//	return db.SUnionStore(cmdStrings[1:])
//}
//
//func sDiffStoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
//	if len(cmdStrings) < 2 {
//		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
//	}
//	return db.SDiffStore(cmdStrings[1:])
//}
//
//func sMoveByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
//	if len(cmdStrings) < 2 {
//		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
//	}
//	return db.SMove(cmdStrings[1:])
//}

/*
	SADD key member [member ...]: 向集合中添加一个或多个成员。
	SREM key member [member ...]: 从集合中移除一个或多个成员。
	SISMEMBER key member: 检查指定成员是否存在于集合中。
	SCARD key: 返回集合中的成员数量（基数）。
	SMEMBERS key: 返回集合中的所有成员。
	SRANDMEMBER key [count]: 随机返回集合中的一个或多个成员。
	SPOP key [count]: 随机移除并返回集合中的一个或多个成员。
	SINTER key [key ...]: 返回多个集合的交集。
	SUNION key [key ...]: 返回多个集合的并集。
	SDIFF key [key ...]: 返回第一个集合与其他集合的差集。
	SINTERSTORE destination key [key ...]: 将多个集合的交集存储到一个新的集合中。
	SUNIONSTORE destination key [key ...]: 将多个集合的并集存储到一个新的集合中。
	SDIFFSTORE destination key [key ...]: 将第一个集合与其他集合的差集存储到一个新的集合中。
	SMOVE source destination member: 将指定成员从一个集合移动到另一个集合。
	SRANDMEMBER key [count]: 随机返回集合中的一个或多个成员，可重复。
*/

func (db *DB) SAdd(key string, member []string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KEY IS NOT SET"), nil
		}
		set.Add(member)
		return protocol.MakeStatusReply("OK! THE MEMBERS HAS BEEN SET!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SRem(key string, member []string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KEY IS NOT SET"), nil
		}
		set.Remove(member)
		return protocol.MakeStatusReply("OK! THE MEMBERS HAS BEEN DELETE!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SisMember(key string, member string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KEY IS NOT SET"), nil
		}
		if set.IsMember(member) {
			return protocol.MakeStatusReply("YES! THE MEMBER IS EXIST"), nil
		} else {
			return protocol.MakeStatusReply("NO! THE MEMBER IS NOT EXIST "), nil
		}
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SCard(key string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KET IS NOT SET"), nil
		}
		return protocol.MakeIntReply(int64(set.Card())), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SMembers(key string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KET IS NOT SET"), nil
		}
		return protocol.MakeMultiBulkReply(trans.StringsToBytes(set.Members())), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SPop(key string, count int) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KET IS NOT SET"), nil
		}
		return protocol.MakeStatusReply("OK! SOME KEY MEMBERS HAS BEEN DELETE! NUMBER:" + strconv.Itoa(set.Pop(count))), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

/*
	SINTER key [key ...]: 返回多个集合的交集。
	SUNION key [key ...]: 返回多个集合的并集。
	SDIFF key [key ...]: 返回第一个集合与其他集合的差集。
	SINTERSTORE destination key [key ...]: 将多个集合的交集存储到一个新的集合中。
	SUNIONSTORE destination key [key ...]: 将多个集合的并集存储到一个新的集合中。
	SDIFFSTORE destination key [key ...]: 将第一个集合与其他集合的差集存储到一个新的集合中。
	SMOVE source destination member: 将指定成员从一个集合移动到另一个集合。
*/

func (db *DB) SInter(keys []string) (redis.Reply, *Extra) {
	result := make([]string, 0)
	setSlice := make([]*redisSet.Set, 0)
	for _, key := range keys {
		val, exists := db.data.Get(key)
		if exists {
			setSlice = append(setSlice, val.(*redisSet.Set))
		}
	}
	if len(setSlice) == 0 {
		return protocol.MakeNullMultiBulk(), nil
	}
	m := make(map[string]struct{})
	for _, s := range setSlice[0].Members() {
		m[s] = struct{}{}
	}

	for i := 1; i < len(setSlice); i++ {
		for _, s := range setSlice[i].Members() {
			if _, ok := m[s]; !ok {
				delete(m, s)
			}
		}
	}

	for k, _ := range m {
		result = append(result, k)
	}
	return protocol.MakeMultiBulkReply(trans.StringsToBytes(result)), nil
}

func (db *DB) SUnion(keys []string) (redis.Reply, *Extra) {
	m := make(map[string]struct{})
	setSlice := make([]*redisSet.Set, 0)
	for _, key := range keys {
		val, exists := db.data.Get(key)
		if exists {
			setSlice = append(setSlice, val.(*redisSet.Set))
		}
	}
	if len(setSlice) == 0 {
		return protocol.MakeNullMultiBulk(), nil
	}

	for _, set := range setSlice {
		for _, s := range set.Members() {
			if _, ok := m[s]; !ok {
				m[s] = struct{}{}
			}
		}
	}

	result := make([]string, 0)
	for k, _ := range m {
		result = append(result, k)
	}
	return protocol.MakeMultiBulkReply(trans.StringsToBytes(result)), nil
}

func (db *DB) SDiff(keys []string) (redis.Reply, *Extra) {
	result := make([]string, 0)
	setSlice := make([]*redisSet.Set, 0)
	for _, key := range keys {
		val, exists := db.data.Get(key)
		if exists {
			setSlice = append(setSlice, val.(*redisSet.Set))
		}
	}
	if len(setSlice) == 0 {
		return protocol.MakeNullMultiBulk(), nil
	}
	m := make(map[string]struct{})
	for _, s := range setSlice[0].Members() {
		m[s] = struct{}{}
	}

	for i := 1; i < len(setSlice); i++ {
		for _, s := range setSlice[i].Members() {
			if _, ok := m[s]; !ok {
				result = append(result, s)
			}
		}
	}

	return protocol.MakeMultiBulkReply(trans.StringsToBytes(result)), nil
}

func (db *DB) SRandMember(key string, count int) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists {
		set, ok := val.(*redisSet.Set)
		if !ok {
			return protocol.MakeErrReply("FAILED! THE KET IS NOT SET"), nil
		}
		return protocol.MakeMultiBulkReply(trans.StringsToBytes(set.RandMember(count))), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

//func (db *DB) SInterStore(keys []string) (redis.Reply, *Extra) {
//
//	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
//}
//
//func (db *DB) SUnionStore(keys []string) (redis.Reply, *Extra) {
//	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
//}
//
//func (db *DB) SDiffStore(keys []string) (redis.Reply, *Extra) {
//	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
//}

//func (db *DB) SMove(keys []string) (redis.Reply, *Extra) {
//	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
//}
