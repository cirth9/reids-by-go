package cmd_utils

import (
	"reids-by-go/datastruct/redis_list"
	"reids-by-go/datastruct/set"
	"reids-by-go/interface/datastruct/dict"
	"reids-by-go/interface/datastruct/sorted"
	"reids-by-go/interface/redis"
	"reids-by-go/redis/protocol"
	"strconv"
	"time"
)

func DataToCmd(key string, data any) *protocol.MultiBulkStringReply {
	if data == nil {
		return nil
	}
	var cmd *protocol.MultiBulkStringReply
	switch val := data.(type) {
	case string:
		cmd = stringToCmd(key, val)
	case *redis_list.List:
		cmd = listToCmd(key, val)
	case *set.Set:
		cmd = setToCmd(key, val)
	case dict.Dict:
		cmd = hashToCmd(key, val)
	case sorted.SortedSetInter:
		cmd = zSetToCmd(key, val)
	}
	return cmd
}

func stringToCmd(key string, data string) *protocol.MultiBulkStringReply {
	return &protocol.MultiBulkStringReply{Args: [][]byte{
		[]byte("set"),
		[]byte(key),
		[]byte(data),
	}}
}

func listToCmd(key string, data *redis_list.List) *protocol.MultiBulkStringReply {
	return nil
}

func setToCmd(key string, data *set.Set) *protocol.MultiBulkStringReply {
	return nil
}

func hashToCmd(key string, data dict.Dict) *protocol.MultiBulkStringReply {
	return nil
}

func zSetToCmd(key string, data sorted.SortedSetInter) *protocol.MultiBulkStringReply {
	return nil
}

var pExpireAtBytes = []byte("PEXPIREAT")

func MakeExpireCmd(key string, expireAt time.Time) redis.Reply {
	args := make([][]byte, 3)
	args[0] = pExpireAtBytes
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))
	return protocol.MakeMultiBulkReply(args)
}
