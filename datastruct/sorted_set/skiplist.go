package sorted_set

import "math/rand"

const maxLevel = 16

type Element struct {
	Member string
	Score  float64
}

type Level struct {
	forward *Node
	span    int64
}

type Node struct {
	Element
	backward *Node
	level    []*Level
}

type skipList struct {
	header *Node
	tail   *Node
	length int64
	level  int64
}

func NewNode(level int16, member string, score float64) *Node {
	node := &Node{
		Element: Element{
			Member: member,
			Score:  score,
		},
		backward: nil,
		level:    make([]*Level, level),
	}
	for i := range node.level {
		node.level[i] = new(Level)
	}
	return node
}

func newSkipList() *skipList {
	return &skipList{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

func (skip *skipList) getByRank(rank int64) *Node {
	var i int64 = 0
	n := skip.header
	for level := skip.level; level >= 0; level-- {
		for n.level[level].forward != nil && (i+n.level[level].span <= rank) {
			i += n.level[level].span
			n = n.level[level].forward
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (skip *skipList) getFirstScoreRange(min, max *ScoreBorder) *Node {
	if skip.hasInRange(min, max) {
		return nil
	}
	n := skip.header
	for level := skip.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && min.less(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	n = n.level[0].forward
	if !max.greater(&n.Element) {
		return nil
	}
	return nil
}

func (skip *skipList) hasInRange(min Border, max Border) bool {
	tail := skip.tail
	if tail == nil || min.greater(&tail.Element) {
		return false
	}
	header := skip.header
	if header == nil || max.less(&header.Element) {
		return false
	}
	return true
}

func (skip *skipList) getFirstInRange(min Border, max Border) *Node {
	if !skip.hasInRange(min, max) {
		return nil
	}
	n := skip.header

	for level := skip.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && !min.less(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}

	n = n.level[0].forward
	if !max.greater(&n.Element) {
		return nil
	}
	return n
}

func (skip *skipList) getLastInRange(min Border, max Border) *Node {
	if !skip.hasInRange(min, max) {
		return nil
	}
	n := skip.header
	// scan from top level
	for level := skip.level - 1; level >= 0; level-- {
		for n.level[level].forward != nil && max.greater(&n.level[level].forward.Element) {
			n = n.level[level].forward
		}
	}
	if !min.less(&n.Element) {
		return nil
	}
	return n
}

func (skip *skipList) insert(member string, score float64) *Node {
	update := make([]*Node, maxLevel) // link new node with node in `update`
	rank := make([]int64, maxLevel)

	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		if i == skip.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if node.level[i] != nil {
			for node.level[i].forward != nil &&
				(node.level[i].forward.Score < score ||
					(node.level[i].forward.Score == score && node.level[i].forward.Member < member)) { // same score, different key
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}

	level := randomLevel()
	if level > skip.level {
		for i := skip.level; i < level; i++ {
			rank[i] = 0
			update[i] = skip.header
			update[i].level[i].span = skip.length
		}
		skip.level = level
	}

	node = makeNode(level, score, member)
	for i := int64(0); i < level; i++ {
		node.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = node

		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < skip.level; i++ {
		update[i].level[i].span++
	}

	// set backward node
	if update[0] == skip.header {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		skip.tail = node
	}
	skip.length++
	return node
}

func (skip *skipList) removeNode(node *Node, update []*Node) {
	for i := int64(0); i < skip.level; i++ {
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else {
		skip.tail = node.backward
	}
	for skip.level > 1 && skip.header.level[skip.level-1].forward == nil {
		skip.level--
	}
	skip.length--
}

func (skip *skipList) remove(member string, score float64) bool {
	update := make([]*Node, maxLevel)
	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil &&
			(node.level[i].forward.Score < score ||
				(node.level[i].forward.Score == score &&
					node.level[i].forward.Member < member)) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Member == member {
		skip.removeNode(node, update)
		// free x
		return true
	}
	return false
}

func (skip *skipList) getRank(member string, score float64) int64 {
	var rank int64 = 0
	x := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Member == member {
			return rank
		}
	}
	return 0
}

func (skip *skipList) RemoveRange(min Border, max Border, limit int) []*Element {
	update := make([]*Node, maxLevel)
	removed := make([]*Element, 0)

	node := skip.header
	for i := skip.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil {
			if min.less(&node.level[i].forward.Element) {
				break
			}
			node = node.level[i].forward
		}
		update[i] = node
	}

	node = node.level[0].forward

	for node != nil {
		if !max.greater(&node.Element) {
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skip.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

func (skip *skipList) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
	var i int64 = 0 // rank of iterator
	update := make([]*Node, maxLevel)
	removed = make([]*Element, 0)

	node := skip.header
	for level := skip.level - 1; level >= 0; level-- {
		for node.level[level].forward != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forward
		}
		update[level] = node
	}

	i++
	node = node.level[0].forward

	for node != nil && i < stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		skip.removeNode(node, update)
		node = next
		i++
	}
	return removed
}

func makeNode(level int64, score float64, member string) *Node {
	n := &Node{
		Element: Element{
			Score:  score,
			Member: member,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func randomLevel() int64 {
	level := int64(1)
	for float32(rand.Int31()&0xFFFF) < (0.25 * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}
