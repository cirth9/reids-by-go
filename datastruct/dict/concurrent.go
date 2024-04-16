package dict

import (
	"hash/fnv"
	"log"
	"math"
	"sort"
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

// RWLocks locks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(hashFnv64(wKey))
		writeIndexSet[uint32(idx)] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

// RWUnLocks unlocks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(hashFnv64(wKey))
		writeIndexSet[uint32(idx)] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := dict.spread(hashFnv64(key))
		indexMap[uint32(index)] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
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
