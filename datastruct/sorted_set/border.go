package sorted_set

type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue(element *Element) any
	getExclude(element *Element) bool
	isIntersected(max Border) bool
}

type ScoreBorder struct {
	Inf     int8
	Value   float64
	Exclude bool
}

func (b *ScoreBorder) greater(element Element) {

}
