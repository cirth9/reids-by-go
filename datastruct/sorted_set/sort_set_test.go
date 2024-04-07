package sorted_set

import (
	"testing"
)

func TestSortedSet_PopMin(t *testing.T) {
	var set = New()
	set.Add("s1", 1)
	set.Add("s2", 2)
	set.Add("s3", 3)
	set.Add("s4", 4)

	//var results = set.PopMin(2)
	//t.Log(set.Get("s1"))
	//t.Log(set.Delete("s1"))
	t.Log(set.Get("s1"))
	t.Log(set.Get("s2"))
	t.Log(set.Get("s3"))
	t.Log(set.Get("s4"))

	elements := set.Range(&ScoreBorder{
		Value: 1,
	}, &ScoreBorder{
		Value: 4,
	}, 0, -1, false)
	//t.Log(set.GetRank("s2", true))
	for _, element := range elements {
		t.Log(element, "     range")
	}
	//t.Log(set.RemoveRange(&ScoreBorder{
	//	Inf:     0,
	//	Value:   2,
	//	Exclude: false,
	//}, &ScoreBorder{
	//	Inf:     0,
	//	Value:   3,
	//	Exclude: false,
	//}))
	//t.Log(set.PopMin(2))

	//elements := set.RangeByRank(0, 2, false)
	//for _, element := range elements {
	//	t.Log(element)
	//}
	//
	//t.Log(set.Update("s1", 99.9))
	//
	//t.Log(set.RangeCount(&ScoreBorder{
	//	Inf:     -1,
	//	Value:   0,
	//	Exclude: false,
	//}, &ScoreBorder{
	//	Inf:     1,
	//	Value:   0,
	//	Exclude: false,
	//}))

	t.Log(set.Get("s1"))
	t.Log(set.Get("s2"))
	t.Log(set.Get("s3"))
	t.Log(set.Get("s4"))
}
