package sorted

import "reids-by-go/datastruct/sorted_set"

type SortedSetInter interface {
	Len() int
	Add(member string, score float64) bool
	Delete(member string) bool
	Update(member string, score float64) bool
	Get(member string) (element *sorted_set.Element, ok bool)
	RemoveByRank(start int64, stop int64) int64
	PopMin(count int) []*sorted_set.Element
	RemoveRange(min sorted_set.Border, max sorted_set.Border) int64
	Range(min sorted_set.Border, max sorted_set.Border, offset int64, limit int64, desc bool) []*sorted_set.Element
	ForEach(min sorted_set.Border, max sorted_set.Border, offset int64, limit int64, desc bool, consumer func(element *sorted_set.Element) bool)
	RangeCount(min sorted_set.Border, max sorted_set.Border) int64
	RangeByRank(start int64, stop int64, desc bool) []*sorted_set.Element
	ForEachByRank(start int64, stop int64, desc bool, consumer func(element *sorted_set.Element) bool)
	GetRank(member string, desc bool) (rank int64)
}
