package consistentHash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

const DefaultReplicas = 3

type Hash func([]byte) uint32

type ConsistentHash struct {
	hash     Hash
	replicas int
	keys     []int
	hashMap  map[int]string
}

func NewConsistentHash(replicas int, fn Hash) *ConsistentHash {
	m := &ConsistentHash{
		replicas: replicas,
		hash:     fn,
		keys:     make([]int, 0),
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (h *ConsistentHash) Add(keys ...string) {
	for _, v := range keys {
		for i := 0; i < h.replicas; i++ {
			hashNumber := h.hash([]byte(strconv.Itoa(i) + v))
			h.keys = append(h.keys, int(hashNumber))
			h.hashMap[int(hashNumber)] = v
		}
	}
	sort.Ints(h.keys)
}

func (h *ConsistentHash) Get(key string) string {
	if len(h.keys) == 0 {
		return ""
	}
	hashNumber := int(h.hash([]byte(key)))
	n := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hashNumber
	})
	return h.hashMap[h.keys[n%len(h.keys)]]
}

func (h *ConsistentHash) Del(key string) {
	for i := 0; i < h.replicas; i++ {
		hashNumber := h.hash([]byte(strconv.Itoa(i) + key))
		h.keys = removeElement(h.keys, int(hashNumber))
		delete(h.hashMap, int(hashNumber))
	}
}

func removeElement(slice []int, element int) []int {
	for i := 0; i < len(slice); i++ {
		if slice[i] == element {
			//todo 使用切片的切片操作将元素从切片中移除
			slice = append(slice[:i], slice[i+1:]...)
			//todo 减小索引，以便继续遍历后续元素
			i--
		}
	}
	return slice
}
