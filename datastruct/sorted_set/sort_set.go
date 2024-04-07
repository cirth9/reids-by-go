package sorted_set

import (
	"strconv"
)

type SortedSet struct {
	dict map[string]*Element
	skip *skipList
}

func (s *SortedSet) Len() int {
	return len(s.dict)
}

func New() *SortedSet {
	return &SortedSet{
		dict: make(map[string]*Element),
		skip: newSkipList(),
	}
}

func (s *SortedSet) Add(member string, score float64) bool {
	if s == nil {
		panic("sorted set is nil!")
	}
	element, ok := s.dict[member]
	s.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			s.skip.remove(member, element.Score)
			s.skip.insert(member, score)
			return true
		}
		return false
	} else {
		s.skip.insert(member, score)
	}
	return true
}

func (s *SortedSet) Delete(member string) bool {
	if s == nil {
		panic("sorted set is nil!")
	}
	if v, ok := s.dict[member]; ok {
		s.skip.remove(member, v.Score)
		delete(s.dict, member)
		return true
	}
	return false
}

func (s *SortedSet) Update(member string, score float64) bool {
	if s == nil {
		panic("sorted set is nil!")
	}
	element, ok := s.dict[member]
	s.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			s.skip.remove(member, element.Score)
			s.skip.insert(member, score)
			return true
		}
	}
	return false
}

func (s *SortedSet) Get(member string) (*Element, bool) {
	if s == nil {
		panic("sorted set is nil!")
	}
	element, ok := s.dict[member]
	if ok {
		return element, ok
	}
	return nil, false
}

func (s *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	if s == nil {
		panic("sorted set is nil!")
	}
	removed := s.skip.RemoveRangeByRank(start, stop)
	return int64(len(removed))
}

func (s *SortedSet) PopMin(count int) []*Element {
	if s == nil {
		panic("sorted set is nil!")
	}
	first := s.skip.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		Value:   first.Score,
		Exclude: false,
	}
	removed := s.skip.RemoveRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(s.dict, element.Member)
	}
	return removed
}

func (s *SortedSet) RemoveRange(min Border, max Border) int64 {
	if s == nil {
		panic("sorted set is nil!")
	}
	removeRange := s.skip.RemoveRange(min, max, 0)
	for _, element := range removeRange {
		delete(s.dict, element.Member)
	}
	return int64(len(removeRange))
}

func (s *SortedSet) Range(min Border, max Border, offset int64, limit int64, desc bool) []*Element {
	if s == nil {
		panic("sorted set is nil!")
	}
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	s.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

func (s *SortedSet) ForEach(min Border, max Border, offset int64, limit int64, desc bool, consumer func(element *Element) bool) {
	if s == nil {
		panic("sorted set is nil!")
	}

	var node *Node
	if desc {
		node = s.skip.getLastInRange(min, max)
	} else {
		node = s.skip.getFirstInRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		gtMin := min.less(&node.Element)
		ltMax := max.greater(&node.Element)
		if !gtMin || !ltMax {
			break
		}
	}
}

func (s *SortedSet) RangeCount(min Border, max Border) int64 {
	if s == nil {
		panic("sorted set is nil!")
	}
	var i int64 = 0
	// ascending order
	s.ForEachByRank(0, int64(s.Len()), false, func(element *Element) bool {
		gtMin := min.less(element) // greater than min
		if !gtMin {
			// has not into range, continue foreach
			return true
		}
		ltMax := max.greater(element) // less than max
		if !ltMax {
			// break through score border, break foreach
			return false
		}
		// gtMin && ltMax
		i++
		return true
	})
	return i
}

func (s *SortedSet) RangeByRank(start int64, stop int64, desc bool) []*Element {
	if s == nil {
		panic("sorted set is nil!")
	}
	sliceSize := int(stop - start)
	slice := make([]*Element, sliceSize)
	i := 0
	s.ForEachByRank(start, stop, desc, func(element *Element) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

func (s *SortedSet) ForEachByRank(start int64, stop int64, desc bool, consumer func(element *Element) bool) {
	if s == nil {
		panic("sorted set is nil!")
	}
	size := int64(s.Len())
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	// find start node
	var node *Node
	if desc {
		node = s.skip.tail
		if start > 0 {
			node = s.skip.getByRank(size - start)
		}
	} else {
		node = s.skip.header.level[0].forward
		if start > 0 {
			node = s.skip.getByRank(start + 1)
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

func (s *SortedSet) GetRank(member string, desc bool) (rank int64) {
	if s == nil {
		panic("sorted set is nil!")
	}
	element, ok := s.dict[member]
	if !ok {
		return -1
	}
	r := s.skip.getRank(member, element.Score)
	if desc {
		r = s.skip.length - r
	} else {
		r--
	}
	return r
}
