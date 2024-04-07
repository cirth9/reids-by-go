package database

import (
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
)

func sAddByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SAdd(cmdStrings[1])
}

func sRemByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SRem(cmdStrings[1])
}

func sisMemberByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SisMember(cmdStrings[1])
}

func sRangeMemberByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SRangeMember(cmdStrings[1])
}

func sPopByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SPop(cmdStrings[1])
}

func sInterByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SInter(cmdStrings[1])
}

func sUnionByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SUnion(cmdStrings[1])
}

func sDiffByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SDiff(cmdStrings[1])
}

func sInterStoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SInterStore(cmdStrings[1])
}

func sUnionStoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SUnionStore(cmdStrings[1])
}

func sDiffStoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SDiffStore(cmdStrings[1])
}

func sMoveByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SMove(cmdStrings[1])
}

func sCardByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SCard(cmdStrings[1])
}

func sMembersByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SMembers(cmdStrings[1])
}

func sRandMemberByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.SRandMember(cmdStrings[1])
}

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

func (db *DB) SAdd(key string, member ...string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SRem(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SisMember(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SCard(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SMembers(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SRangeMember(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SPop(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SInter(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SUnion(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SDiff(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SInterStore(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SUnionStore(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SDiffStore(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SMove(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) SRandMember(key string) (redis.Reply, *Extra) {
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}
