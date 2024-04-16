package database

import (
	"reflect"
	"reids-by-go/datastruct/redis_list"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/trans"
	"strconv"
	"strings"
)

func lPushByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LPush(cmdStrings[1], cmdStrings[2:])
}

func lPopByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LPop(cmdStrings[1])
}

func rPopByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.RPop(cmdStrings[1])
}

func lIndexByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LIndex(cmdStrings[1], cmdStrings[2])
}

func lLenByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LLen(cmdStrings[1])
}

func lRangeByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LRange(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func lInsertByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 5 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LInsert(cmdStrings[1], cmdStrings[2], cmdStrings[3], cmdStrings[4])
}

func lSetByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LSet(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func lRemByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LRem(cmdStrings[1], cmdStrings[2])
}

func lTrimByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.LTrim(cmdStrings[1], cmdStrings[2], cmdStrings[3])
}

func rPushByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.RPush(cmdStrings[1], cmdStrings[2:])
}

func (db *DB) LPush(key string, vals ...any) (redis.Reply, *Extra) {
	var reply strings.Builder
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		for _, val := range vals {
			list.LeftPush(val)
			reply.WriteString(reflect.ValueOf(val).String())
			reply.WriteString(" ")
		}
		return protocol.MakeStatusReply("OK! LIST KEY " + key + " ADD: " + reply.String()), nil
	}
	list := redis_list.NewRedisList()
	for _, val := range vals {
		list.LeftPush(val)
		reply.WriteString(reflect.ValueOf(val).String())
		reply.WriteString(" ")
	}
	db.data.Put(key, list)
	return protocol.MakeStatusReply("OK! LIST KEY " + key + " ADD: " + reply.String()), nil
}

func (db *DB) RPush(key string, vals ...any) (redis.Reply, *Extra) {
	var reply strings.Builder
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		for _, val := range vals {
			list.RightPush(val)
			reply.WriteString(reflect.ValueOf(val).String())
			reply.WriteString(" ")
		}
		return protocol.MakeStatusReply("OK! LIST KEY " + key + " ADD: " + reply.String()), nil
	}
	list := redis_list.NewRedisList()
	for _, val := range vals {
		list.RightPush(val)
		reply.WriteString(reflect.ValueOf(val).String())
		reply.WriteString(" ")
	}
	db.data.Put(key, list)
	return protocol.MakeStatusReply("OK! LIST KEY " + key + " ADD: " + reply.String()), nil
}

func (db *DB) LPop(key string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}

		if list.LeftPop() {
			return protocol.MakeStatusReply("OK! LIST KEY " + key + " POP "), nil
		} else {
			return protocol.MakeStatusReply("FAILED! LIST HAVE NO ELEMENT!"), nil
		}
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) RPop(key string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}

		if list.RightPop() {
			return protocol.MakeStatusReply("OK! LIST KEY " + key + " POP "), nil
		} else {
			return protocol.MakeStatusReply("FAILED! LIST HAVE NO ELEMENT!"), nil
		}
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LIndex(key string, index string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}

		indexNumber, err := strconv.Atoi(index)
		if err != nil {
			return protocol.MakeStatusReply("FAILED! INDEX FORMAT ERROR!"), nil
		}
		indexValue := list.IndexValue(indexNumber)
		if indexValue == nil {
			return protocol.MakeStatusReply("FAILED! CAN'T FIND THE INDEX's VALUE!"), nil
		}
		return protocol.MakeBulkReply(trans.AnyToBytes(indexValue)), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LLen(key string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		return protocol.MakeIntReply(int64(list.Len())), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LRange(key string, start, end string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		startIndex, err := strconv.Atoi(start)
		endIndex, err := strconv.Atoi(end)
		if err != nil {
			return protocol.MakeStatusReply("FAILED! START OR END FORMAT ERROR!"), nil
		}
		values := list.Range(startIndex, endIndex)
		protocol.MakeMultiBulkReply(trans.AnysToBytes(values))
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LInsert(key string, flag string, pivot, value any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}

		//todo 处理flag
		if flag == "before" {
			if list.InsertBefore(pivot, value) {
				return protocol.MakeStatusReply("OK! LIST KEY " + key + " INSERT "), nil
			} else {
				return protocol.MakeStatusReply("FAILED! THE PIVOT DO NOT EXISTED!"), nil
			}
		} else if flag == "after" {
			if list.InsertAfter(pivot, value) {
				return protocol.MakeStatusReply("OK! LIST KEY " + key + " INSERT "), nil
			} else {
				return protocol.MakeStatusReply("FAILED! THE PIVOT DO NOT EXISTED!"), nil
			}
		}

		return protocol.MakeStatusReply("FAILED! NO FLAG!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LSet(key string, index string, val any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		indexNumber, err := strconv.Atoi(index)
		if err != nil {
			return protocol.MakeStatusReply("FAILED! INDEX FORMAT ERROR!"), nil
		}
		if list.Set(indexNumber, val) {
			return protocol.MakeStatusReply("OK! SET INDEX " + index + " VALUE " + trans.AnyToString(val)), nil
		}
		return protocol.MakeStatusReply("FAILED! SAME AS BEFORE OR HAVE NO INDEX!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LRem(key string, val any) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		if list.Remove(val) {
			return protocol.MakeStatusReply("OK! THE VALUE'S ELEMENT HAVE BEEN REMOVED!"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE VALUE'S ELEMENT MAY NOT EXISTED!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) LTrim(key string, start, end string) (redis.Reply, *Extra) {
	value, exists := db.data.Get(key)
	if exists {
		list, ok := value.(*redis_list.List)
		if !ok {
			return protocol.MakeStatusReply("FAILED! THE KEY'S TYPE IS NOT LIST!"), nil
		}
		startIndex, err := strconv.Atoi(start)
		endIndex, err := strconv.Atoi(end)
		if err != nil {
			return protocol.MakeStatusReply("FAILED! START OR END FORMAT ERROR!"), nil
		}
		if list.Trim(startIndex, endIndex) {
			return protocol.MakeStatusReply("OK! THE LIST HAVE BEEN TRIM! START " + start + " END " + end), nil
		}
		return protocol.MakeStatusReply("FAILED! THE VALUE'S ELEMENT MAY NOT EXISTED!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

/*
LPUSH key value [value ...]: 将一个或多个值插入到链表的头部（左侧）。
RPUSH key value [value ...]: 将一个或多个值插入到链表的尾部（右侧）。
LPOP key: 移除并返回链表头部的元素。
RPOP key: 移除并返回链表尾部的元素。
LINDEX key index: 返回链表中指定索引位置的元素。
LLEN key: 返回链表的长度（即节点数量）。
LRANGE key start stop: 返回链表中指定范围内的元素列表。
LINSERT key BEFORE|AFTER pivot value: 在链表中找到指定元素 pivot，并在其前或后插入新元素 value。
LSET key index value: 设置链表中指定索引位置的元素的值。
LREM key count value: 从链表中删除指定个数的匹配元素。
LTRIM key start stop: 修剪链表，只保留指定范围内的元素。
RPOPLPUSH source destination: 移除源链表的尾部元素，并将其插入到目标链表的头部。
*/

func lPushRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	//rollback = append(rollback, []byte("lpop"))
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	list, ok := val.(*redis_list.List)
	if !ok {
		return nil
	}
	length, cnt := list.Len(), 0
	for i := 2; i < len(cmdLine); i++ {
		cnt++
	}
	rollback = append(rollback, []byte("ltrim"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, []byte(strconv.Itoa(cnt)))
	rollback = append(rollback, []byte(strconv.Itoa(length-1)))
	return rollback
}

func rPushRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	//rollback = append(rollback, []byte("lpop"))
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	list, ok := val.(*redis_list.List)
	if !ok {
		return nil
	}
	length, cnt := list.Len(), 0
	for i := 2; i < len(cmdLine); i++ {
		cnt++
	}
	rollback = append(rollback, []byte("ltrim"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, []byte(strconv.Itoa(0)))
	rollback = append(rollback, []byte(strconv.Itoa(length-cnt-1)))
	return rollback
}

func lPopRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	list, ok := val.(*redis_list.List)
	if !ok {
		return nil
	}
	headerElement := list.Header()
	rollback = append(rollback, []byte("lpush"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, trans.AnyToBytes(headerElement))
	return rollback
}

func rPopRollBack(cmdLine CmdLine, db *DB) CmdLine {
	rollback := make(CmdLine, 0)
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	list, ok := val.(*redis_list.List)
	if !ok {
		return nil
	}

	headerElement := list.Header()
	rollback = append(rollback, []byte("rpush"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, trans.AnyToBytes(headerElement))
	return rollback
}

func lInsertRollBack(cmdLine CmdLine, db *DB) CmdLine {
	// LINSERT key BEFORE|AFTER pivot value
	rollback := make(CmdLine, 0)
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	_, ok := val.(*redis_list.List)
	if !ok {
		return nil
	}
	//LREM key count value:
	rollback = append(rollback, []byte("lrem"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, []byte("1"))
	rollback = append(rollback, cmdLine[4])
	return rollback
}

func lSetRollBack(cmdLine CmdLine, db *DB) CmdLine {
	// LSET key index value: 设置链表中指定索引位置的元素的值。
	rollback := make(CmdLine, 0)
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	list, ok := val.(*redis_list.List)
	if !ok {
		return nil
	}
	index, err := strconv.Atoi(string(cmdLine[2]))
	if err != nil {
		return nil
	}
	value := list.GetValue(index)
	rollback = append(rollback, []byte("lset"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, cmdLine[2])
	rollback = append(rollback, trans.AnyToBytes(value))
	return rollback
}

func lRemRollBack(cmdLine CmdLine, db *DB) CmdLine {
	// LREM key count value: 从链表中删除指定个数的匹配元素。
	rollback := make(CmdLine, 0)
	return rollback
}

func lTrimRollBack(cmdLine CmdLine, db *DB) CmdLine {
	// LTRIM key start stop: 修剪链表，只保留指定范围内的元素。
	rollback := make(CmdLine, 0)
	return rollback
}
