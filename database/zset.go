package database

import (
	"errors"
	"reids-by-go/datastruct/sorted_set"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"reids-by-go/utils/trans"
	"strconv"
	"strings"
)

func zAddByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zadd(cmdStrings)
}

func zRemByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zrem(cmdStrings)
}

func zRangeByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zrange(cmdStrings)
}

func zRangeByLexByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zrange(cmdStrings)
}

func zRankByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zrank(cmdStrings)
}

func zScoreByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 3 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zscore(cmdStrings)
}

func zCountByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zcount(cmdStrings)
}

func zIncByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 4 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.ZincBy(cmdStrings)
}

func zCardByDb(cmdStrings []string, db *DB) (redis.Reply, *Extra) {
	if len(cmdStrings) < 2 {
		return protocol.MakeErrReply("COMMAND'S PARAMS NUMBER ERROR"), nil
	}
	return db.Zcard(cmdStrings)
}

func (db *DB) Zadd(cmdStrings []string) (redis.Reply, *Extra) {
	var elements []*sorted_set.Element
	length := len(cmdStrings)
	if length%2 != 0 {
		return protocol.MakeErrReply(errors.New("ZADD CMD FORMAT ERROR").Error()), nil
	}
	for i := 2; i < length; i += 2 {
		score, err := strconv.ParseFloat(cmdStrings[i], 64)
		if err != nil {
			return protocol.MakeErrReply(err.Error()), nil
		}
		member := cmdStrings[i+1]
		elements = append(elements, &sorted_set.Element{
			Member: member,
			Score:  score,
		})
	}
	return db.zadd(cmdStrings[1], elements)
}

func (db *DB) zadd(key string, elements []*sorted_set.Element) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	//log.Println("existed ", exists)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		for _, element := range elements {
			sortedSet.Add(element.Member, element.Score)
		}
		return protocol.MakeStatusReply("OK! KEY " + key + " ADD:" + strconv.Itoa(len(elements))), nil
	} else {
		//todo key is not existed
		sortedSet := sorted_set.New()
		for _, element := range elements {
			sortedSet.Add(element.Member, element.Score)
		}
		db.data.Put(key, sortedSet)
		return protocol.MakeStatusReply("OK! NEW KEY " + key + " ADD:" + strconv.Itoa(len(elements))), nil
	}
}

func (db *DB) Zrem(cmdStrings []string) (redis.Reply, *Extra) {
	var elements []*sorted_set.Element
	length := len(cmdStrings)
	for i := 2; i < length; i++ {
		elements = append(elements, &sorted_set.Element{
			Member: cmdStrings[i],
		})
	}
	return db.zrem(cmdStrings[1], elements)
}

func (db *DB) zrem(key string, elements []*sorted_set.Element) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		for _, element := range elements {
			sortedSet.Delete(element.Member)
		}
		return protocol.MakeStatusReply("OK! KEY " + key + " DELETE:" + strconv.Itoa(len(elements))), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

// Zrange todo ZRANGE and ZRANGEBYLEX
func (db *DB) Zrange(cmdStrings []string) (redis.Reply, *Extra) {
	//zrange key min max [flag]
	var errCount int
	var flag bool
	length := len(cmdStrings)
	if length != 5 {
		flag = false
	} else {
		if strings.ToLower(cmdStrings[length-1]) == "withscore" {
			flag = true
		} else {
			flag = false
		}
	}

	minVal, err := strconv.ParseFloat(cmdStrings[2], 64)
	if err != nil {
		errCount++
	}
	maxVal, err := strconv.ParseFloat(cmdStrings[3], 64)
	if err != nil {
		errCount++
	}
	cmd := strings.ToLower(cmdStrings[0])
	if errCount == 0 && cmd == zRange {
		return db.zrange(cmdStrings[1], &sorted_set.ScoreBorder{
			Value: minVal,
		}, &sorted_set.ScoreBorder{
			Value: maxVal,
		}, 0, -1, flag, false)
	} else if cmd == zRangeByLex {
		return db.zrange(cmdStrings[1], &sorted_set.LexBorder{
			Value: cmdStrings[2],
		}, &sorted_set.LexBorder{
			Value: cmdStrings[3],
		}, 0, -1, flag, false)
	}
	return protocol.MakeErrReply(errors.New("CMD PARSE ERROR").Error()), nil
}

func (db *DB) zrange(key string, min sorted_set.Border, max sorted_set.Border, offset, limit int64, flag, reverse bool) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	//log.Println("zrange", exists)
	if exists && val != nil {
		bytesReply := make([][]byte, 0)
		sortedSet := val.(*sorted_set.SortedSet)
		elements := sortedSet.Range(min, max, offset, limit, false)
		if elements == nil {
			return protocol.MakeNullMultiBulk(), nil
		}
		if flag {
			for _, element := range elements {
				//log.Println(element)
				bytesReply = append(bytesReply, []byte("key: "+element.Member+"   value: "+strconv.FormatFloat(element.Score, 'g', -1, 64)))
			}
		} else {
			for _, element := range elements {
				//log.Println(element)
				bytesReply = append(bytesReply, []byte("key: "+element.Member))
			}
		}
		return protocol.MakeMultiBulkReply(bytesReply), nil
	}
	return protocol.MakeNullMultiBulk(), nil
} //todo 可同时兼顾lex and score

func (db *DB) Zrank(cmdStrings []string) (redis.Reply, *Extra) {
	// zrank key member [reverse]
	length := len(cmdStrings)
	if strings.ToLower(cmdStrings[length-1]) == "reverse" && length == 4 {
		return db.zrank(cmdStrings[1], &sorted_set.Element{
			Member: cmdStrings[2],
		}, true)
	} else {
		return db.zrank(cmdStrings[1], &sorted_set.Element{
			Member: cmdStrings[2],
		}, false)
	}
}

func (db *DB) zrank(key string, element *sorted_set.Element, reverse bool) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		rank := sortedSet.GetRank(element.Member, reverse)
		return protocol.MakeIntReply(rank), nil
	}
	return protocol.MakeIntReply(-1), nil
}

func (db *DB) Zscore(cmdStrings []string) (redis.Reply, *Extra) {
	//zscore key member
	return db.zscore(cmdStrings[1], &sorted_set.Element{
		Member: cmdStrings[2],
	})
}

func (db *DB) zscore(key string, element *sorted_set.Element) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		get, ok := sortedSet.Get(element.Member)
		if ok && get != nil {
			return protocol.MakeFloatReply(get.Score), nil
		}
		return protocol.MakeFloatReply(-1), nil
	}
	return protocol.MakeFloatReply(-1), nil
}

func (db *DB) Zcount(cmdStrings []string) (redis.Reply, *Extra) {
	//zcount key min max
	var err error
	minVal, err := strconv.ParseFloat(cmdStrings[2], 64)
	maxVal, err := strconv.ParseFloat(cmdStrings[3], 64)
	if err != nil {
		return db.zcount(cmdStrings[1], &sorted_set.LexBorder{
			Value: cmdStrings[2],
		}, &sorted_set.LexBorder{
			Value: cmdStrings[3],
		})
	} else {
		return db.zcount(cmdStrings[1], &sorted_set.ScoreBorder{
			Value: minVal,
		}, &sorted_set.ScoreBorder{
			Value: maxVal,
		})
	}
}

func (db *DB) zcount(key string, min sorted_set.Border, max sorted_set.Border) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		count := sortedSet.RangeCount(min, max)
		return protocol.MakeIntReply(count), nil
	}
	return protocol.MakeIntReply(-1), nil
}

func (db *DB) ZincBy(cmdStrings []string) (redis.Reply, *Extra) {
	//ZINCRBY key increment member
	inc, err := strconv.ParseFloat(cmdStrings[2], 64)
	if err != nil {
		return protocol.MakeErrReply("ZINCBY INCREMENT PARSE ERROR:" + err.Error()), nil
	}
	return db.zincBy(cmdStrings[1], &sorted_set.Element{
		Member: cmdStrings[3],
	}, inc)
}

func (db *DB) zincBy(key string, element *sorted_set.Element, inc float64) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		_, ok := sortedSet.Get(element.Member)
		if ok {
			sortedSet.Delete(element.Member)
			sortedSet.Add(element.Member, element.Score+inc)
			return protocol.MakeStatusReply("OK! KEY " + key + " SCORE INC"), nil
		}
		return protocol.MakeStatusReply("FAILED! THE MEMBER DO NOT EXISTED!"), nil
	}
	return protocol.MakeStatusReply("FAILED! THE KEY DO NOT EXISTED!"), nil
}

func (db *DB) Zcard(cmdStrings []string) (redis.Reply, *Extra) {
	//ZCARD key
	return db.zcard(cmdStrings[1])
}

func (db *DB) zcard(key string) (redis.Reply, *Extra) {
	val, exists := db.data.Get(key)
	if exists && val != nil {
		sortedSet := val.(*sorted_set.SortedSet)
		count := sortedSet.RangeCount(&sorted_set.ScoreBorder{
			Inf: -1,
		}, &sorted_set.ScoreBorder{
			Inf: 1,
		})
		return protocol.MakeIntReply(count), nil
	}
	return protocol.MakeIntReply(-1), nil
}

func zAddRollBack(cmdLine CmdLine, db *DB) CmdLine {
	//zrem key  member1 member2 ..
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte("zrem"))
	rollback = append(rollback, cmdLine[1])
	for i := 2; i < len(cmdLine); i += 2 {
		rollback = append(rollback, cmdLine[i+1])
	}
	return rollback
}

func zRemRollBack(cmdLine CmdLine, db *DB) CmdLine {
	//zadd key score1 member1 score2 member2
	val, exists := db.data.Get(string(cmdLine[1]))
	if !exists {
		return nil
	}
	zset, ok := val.(*sorted_set.SortedSet)
	if !ok {
		return nil
	}
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte("zadd"))
	rollback = append(rollback, cmdLine[1])
	for i := 2; i < len(cmdLine); i++ {
		if element, b := zset.Get(string(cmdLine[i])); b {
			rollback = append(rollback, trans.AnyToBytes(element.Score))
			rollback = append(rollback, trans.AnyToBytes(element.Member))
		}
	}
	return rollback
}

func zIncByRollBack(cmdLine CmdLine, db *DB) CmdLine {
	//ZINCRBY key increment member
	inc, err := strconv.ParseFloat(string(cmdLine[2]), 64)
	if err != nil {
		return nil
	}
	rollback := make(CmdLine, 0)
	rollback = append(rollback, []byte("zincby"))
	rollback = append(rollback, cmdLine[1])
	rollback = append(rollback, []byte(strconv.FormatFloat(-inc, 'g', -1, 64)))
	rollback = append(rollback, cmdLine[3])
	return rollback
}
