package lock

import (
	"hash/fnv"
	"sort"
	"sync"
)

// Locks todo 多个key可以共用一个hash捅，Locks主要是用于对哈希槽进行加锁
type Locks struct {
	table []*sync.RWMutex
}

func NewLocks(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	return &Locks{table: table}
}

func (locks *Locks) spread(hashCode uint64) uint64 {
	if locks == nil {
		panic("locks is nil")
	}
	tableSize := uint64(len(locks.table))
	return (tableSize - 1) & hashCode
}

func (locks *Locks) Lock(key string) {
	index := locks.spread(hashFnv64(key))
	mu := locks.table[index]
	mu.Lock()
}

func (locks *Locks) UnLock(key string) {
	index := locks.spread(hashFnv64(key))
	mu := locks.table[index]
	mu.Unlock()
}

// todo 主要是为了解决加锁导致的死锁问题，如果都是按照顺序进行读取则不会出现死锁的问题
func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint64 {
	indexMap := make(map[uint64]bool)
	for _, key := range keys {
		index := locks.spread(hashFnv64(key))
		indexMap[index] = true
	}

	indices := make([]uint64, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		} else {
			return indices[i] < indices[j]
		}
	})
	return indices
}

func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndices := locks.toLockIndices(writeKeys, false)
	writeIndexSet := make(map[uint64]struct{})
	for _, index := range writeIndices {
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndices := locks.toLockIndices(writeKeys, false)
	writeIndexSet := make(map[uint64]struct{})
	for _, index := range writeIndices {
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := locks.table[index]
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

func hashFnv64(key string) uint64 {
	hash64 := fnv.New64a()
	_, _ = hash64.Write([]byte(key))
	hashValue := hash64.Sum64()
	return hashValue
}
