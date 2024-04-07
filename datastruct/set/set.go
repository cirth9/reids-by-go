package set

import "reids-by-go/datastruct/dict"

type intSet struct {
	elements []int
}

type Set struct {
	d        *dict.ConcurrentDict
	i        *intSet
	isIntSet bool
}

/*
	SADD key member [member ...]: 向集合中添加一个或多个成员。
	SREM key member [member ...]: 从集合中移除一个或多个成员。
	SISMEMBER key member: 检查指定成员是否存在于集合中。
	SCARD key: 返回集合中的成员数量（基数）。
	SMEMBERS key: 返回集合中的所有成员。
	SRANDMEMBER key [count]: 随机返回集合中的一个或多个成员。
	SPOP key [count]: 随机移除并返回集合中的一个或多个成员。
	SINTER key [key ...]: 返回多个集合的交集。
	SUNION key [key ...]: 返回多个集合的并集。
	SDIFF key [key ...]: 返回第一个集合与其他集合的差集。
	SINTERSTORE destination key [key ...]: 将多个集合的交集存储到一个新的集合中。
	SUNIONSTORE destination key [key ...]: 将多个集合的并集存储到一个新的集合中。
	SDIFFSTORE destination key [key ...]: 将第一个集合与其他集合的差集存储到一个新的集合中。
	SMOVE source destination member: 将指定成员从一个集合移动到另一个集合。
	SRANDMEMBER key [count]: 随机返回集合中的一个或多个成员，可重复。
*/

func (s *Set) Add(member []any) {

}

func (s *Set) Remove() {

}

func (s *Set) IsMember() {

}

func (s *Set) Card() {

}

func (s *Set) Members() {

}

func (s *Set) RangeMember() {

}

func (s *Set) Pop() {

}

func (s *Set) Inter() {

}

func (s *Set) Union() {

}

func (s *Set) Diff() {

}

func (s *Set) InterStore() {

}

func (s *Set) UnionStore() {

}

func (s *Set) DiffStore() {

}

func (s *Set) Move() {

}

func (s *Set) RandMember() {

}
