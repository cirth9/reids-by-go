package sorted_set

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

	return nil
}
