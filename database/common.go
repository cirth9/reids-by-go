package database

import (
	"log"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/trans"
	"strings"
)

func setByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Set(cmdStrings)
}

func getByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Get(cmdStrings)
}

func mSetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 || (len(cmdStrings)-1)%2 != 0 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.MSet(cmdStrings)
}

func mGetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.MGet(cmdStrings)
}

func deleteByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Delete(cmdStrings)
}

func pingByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	return db.Ping()
}

func (db *DB) Ping() (redis.Reply, *Extra) {
	return protocol.MakeBulkReply([]byte("PONG")), nil
}

func (db *DB) Set(cmdStrings []string) (redis.Reply, *Extra) {
	result := db.data.Put(cmdStrings[1], cmdStrings[2])
	//todo persist
	extra := &Extra{toPersist: result > 0}
	if result > 0 {
		extra.specialAof = []*protocol.MultiBulkStringReply{
			{
				Args: [][]byte{
					[]byte("set"),
					[]byte(cmdStrings[1]),
					[]byte(cmdStrings[2]),
				},
			},
		}
	}
	if result >= 1 {
		return protocol.MakeStatusReply("SET OK , NEW KEY"), extra
	} else {
		return protocol.MakeStatusReply("SET OK , ORIGIN KEY'S VAL HAS BEEN UPDATED!"), extra
	}
}

func (db *DB) Get(cmdStrings []string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(cmdStrings[1])
	//todo persist

	if exists {
		return protocol.MakeBulkReply([]byte(val.(string))), nil
	} else {
		return protocol.MakeBulkReply([]byte("DO NOT EXISTED! KEY:" + cmdStrings[1])), nil
	}
}

func (db *DB) Delete(cmdStrings []string) (redis.Reply, *Extra) {
	result := db.data.Delete(cmdStrings[1])
	//todo persist

	if result == 1 {
		return protocol.MakeStatusReply("DELETE OK"), nil
	} else {
		return protocol.MakeStatusReply("THE KEY IS NOT EXISTED"), nil
	}
}

func (db *DB) MSet(cmdStrings []string) (redis.Reply, *Extra) {
	result := 0
	for i := 1; i < len(cmdStrings); i += 2 {
		if db.data.Put(cmdStrings[i], cmdStrings[i+1]) == 1 {
			result++
		}
	}

	if result >= (len(cmdStrings)-1)/2 {
		return protocol.MakeStatusReply("SET OK , ALL NEW KEY"), nil
	} else {
		return protocol.MakeStatusReply("SET OK , SOME ORIGIN KEY'S VAL HAS BEEN UPDATED!"), nil
	}
}

func (db *DB) MGet(cmdStrings []string) (redis.Reply, *Extra) {
	var result []any
	var unExistKey strings.Builder
	for i := 1; i < len(cmdStrings); i++ {
		if val, exists := db.data.Get(cmdStrings[i]); exists {
			result = append(result, val)
		} else {
			unExistKey.WriteString(cmdStrings[i])
			unExistKey.WriteString(" ")
		}
	}
	log.Println(result)
	if len(result) == len(cmdStrings)-1 {
		return protocol.MakeMultiBulkReply(trans.AnysToBytes(result)), nil
	} else {
		return protocol.MakeStatusReply("FAILED! SOME KEY IS NOT EXIST! KEYS:" + unExistKey.String() + " FIND: " + strings.Join(trans.AnysToStrings(result), "")), nil
	}
}

func deleteRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte(set))
	val, exists := db.data.Get(string(cmdLine[1]))
	if exists {
		rollback = append(rollback, trans.AnyToBytes(val))
	} else {
		return nil
	}
	return rollback
}

func setRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte(deleteKey))
	rollback = append(rollback, cmdLine[1])
	return rollback
}
