package database

import (
	"reids-by-go/database"
	"reids-by-go/interface/redis"
)

type Exec interface {
	Ping() redis.Reply
	RedisCommon
	RedisZset
	RedisExpire
}

type RedisCommon interface {
	Get(cmdStrings []string) (redis.Reply, *database.Extra)
	Set(cmdStrings []string) (redis.Reply, *database.Extra)
	Delete(cmdStrings []string) (redis.Reply, *database.Extra)
}

type RedisZset interface {
	Zadd(cmdStrings []string) (redis.Reply, *database.Extra)
	Zrem(cmdStrings []string) (redis.Reply, *database.Extra)
	//Zrange todo 可同时兼顾lex and score
	Zrange(cmdStrings []string) (redis.Reply, *database.Extra)
	Zrank(cmdStrings []string) (redis.Reply, *database.Extra)
	Zscore(cmdStrings []string) (redis.Reply, *database.Extra)
	Zcount(cmdStrings []string) (redis.Reply, *database.Extra)
	ZincBy(cmdStrings []string) (redis.Reply, *database.Extra)
	Zcard(cmdStrings []string) (redis.Reply, *database.Extra)
}

type RedisExpire interface {
	Expire(cmdStrings []string) (redis.Reply, *database.Extra)
	PExpire(cmdStrings []string) (redis.Reply, *database.Extra)
	ExpireAt(cmdStrings []string) (redis.Reply, *database.Extra)
	PExpireAt(cmdStrings []string) (redis.Reply, *database.Extra)
	TTL(cmdStrings []string) (redis.Reply, *database.Extra)
	PTTL(cmdStrings []string) (redis.Reply, *database.Extra)
	Persist(cmdStrings []string) (redis.Reply, *database.Extra)
}
