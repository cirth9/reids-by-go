package dict

import (
	"hash/fnv"
	"log"
	"math"
	"sync"
	"sync/atomic"
)

type ConcurrentDict struct {
	table []*Shard
	count int32
}

type Shard struct {
	m     map[string]any
	mutex sync.RWMutex
}

// todo 控制hashCode映射到table切片的范围内
func (dict *ConcurrentDict) spread(hashCode uint64) uint64 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint64(len(dict.table))
	return (tableSize - 1) & hashCode
}

// todo 获取指定index的Shard
func (dict *ConcurrentDict) getShard(index uint64) *Shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// todo 初始化规模，这里一直按位取or，右移是为了获取大于该param的最小的2次幂
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return n + 1
	}
}

func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			m: make(map[string]any),
		}
	}
	d := &ConcurrentDict{
		table: table,
		count: 0,
	}
	return d
}

func (dict *ConcurrentDict) Get(key string) (val any, exists bool) {
	if dict == nil {
		panic("dict is nil!")
	}
	hashCode := hashFnv64(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	val, exists = shard.m[key]
	return
}

func (dict *ConcurrentDict) Put(key string, val any) (result int) {
	if dict == nil {
		panic("dict is nil!")
	}
	hashCode := hashFnv64(key)
	index := dict.spread(hashCode)
	shard := dict.table[index]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		//have existed
		shard.m[key] = val
		return 1
	} else {
		//do not exist
		shard.m[key] = val
		//log.Println("put >>> ", key, shard.m[key])
		dict.addCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) PutIfNX(key string, val any) (result int) {
	if dict == nil {
		panic("dict is nil!")
	}
	hashCode := hashFnv64(key)
	index := dict.spread(hashCode)
	shard := dict.table[index]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	if _, ok := shard.m[key]; ok {
		return 0
	} else {
		//do not exist
		shard.m[key] = val
		//log.Println("put >>> ", key, shard.m[key])
		dict.addCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Delete(key string) int {
	if dict == nil {
		panic("dict is nil!")
	}
	hashCode := hashFnv64(key)
	index := dict.spread(hashCode)
	shard := dict.table[index]
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	if _, ok := shard.m[key]; ok {
		delete(shard.m, key)
		dict.lessCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil!")
	}
	//todo atomic.LoadInt32原子操作防止data race
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) addCount() {
	dict.count++
}

func (dict *ConcurrentDict) lessCount() {
	dict.count--
}

func hashFnv64(key string) uint64 {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
		}
	}()

	hash64 := fnv.New64a()
	_, err := hash64.Write([]byte(key))
	if err != nil {
		panic(err)
	}
	hashValue := hash64.Sum64()
	return hashValue
}

type Consumer func(key string, val any) bool

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}
