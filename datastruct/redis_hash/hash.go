package redis_hash

import (
	"regexp"
	"reids-by-go/datastruct/dict"
)

type Hash struct {
	d *dict.ConcurrentDict
}

/*
HSET key field value
HGET key field
HDEL key field1 [field2 ...]
HEXISTS key field
HGETALL key
HINCRBY key field increment
HINCRBYFLOAT key field increment
HKEYS key
HLEN key
HMSET key field1 value1 [field2 value2 ...]
HMGET key field1 [field2 ...]
HSETNX key field value
HVALS key
HSCAN key cursor [MATCH pattern] [COUNT count]
*/

func NewHash() *Hash {
	return &Hash{
		d: dict.MakeConcurrent(1 << 16),
	}
}

func (h *Hash) Set(field string, value any) bool {
	return h.d.Put(field, value) == 1
}

func (h *Hash) Get(field string) (any, bool) {
	return h.d.Get(field)
}

func (h *Hash) Del(field string) bool {
	return h.d.Delete(field) == 1
}

func (h *Hash) Exists(field string) bool {
	_, exists := h.d.Get(field)
	return exists
}

func (h *Hash) GetAll() [][]any {
	all := make([][]any, 0)
	h.d.ForEach(func(key string, val any) bool {
		one := make([]any, 0)
		one = append(one, key, val)
		all = append(all, one)
		return true
	})
	return all
}

func (h *Hash) IncBy(field string, inc any) bool {
	get, ok := h.d.Get(field)
	if ok {
		switch get.(type) {
		case float64:
			h.d.Delete(field)
			h.d.Put(field, get.(float64)+inc.(float64))
			return true
		case int64:
			h.d.Delete(field)
			h.d.Put(field, get.(int64)+inc.(int64))
			return true
		default:
			return false
		}
	}
	return false
}

func (h *Hash) Keys(field string) []string {
	keys := make([]string, 0)
	h.d.ForEach(func(key string, val any) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

func (h *Hash) Len(field string) int {
	return h.d.Len()
}

func (h *Hash) MSet(Kvs map[string]any) int {
	if Kvs == nil || len(Kvs) == 0 {
		return 0
	}
	for key, val := range Kvs {
		h.d.Put(key, val)
	}
	return len(Kvs)
}

func (h *Hash) MGet(field []string) []any {
	vals := make([]any, 0)
	for _, s := range field {
		val, _ := h.d.Get(s)
		vals = append(vals, val)
	}
	return vals
}

func (h *Hash) SetNX(field string, val any) bool {
	result := h.d.PutIfNX(field, val)
	return result == 1
}

func (h *Hash) Vals() []any {
	vals := make([]any, 0)
	h.d.ForEach(func(key string, val any) bool {
		vals = append(vals, val)
		return true
	})
	return vals
}

func (h *Hash) Scan(cursor int, match string, count int) map[string]any {
	nowCursor, cnt, result := 0, 0, make(map[string]any, 0)
	regex, err := regexp.Compile(match)
	if err != nil {
		return nil
	}
	h.d.ForEach(func(key string, val any) bool {
		if nowCursor >= cursor {
			if cnt == count {
				return false
			}
			if regex.MatchString(key) {
				result[key] = val
				cnt++
			}
		}
		nowCursor++
		return true
	})
	return result
}
