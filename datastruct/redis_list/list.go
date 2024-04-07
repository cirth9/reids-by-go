package redis_list

import (
	"container/list"
	"reids-by-go/utils/trans"
)

type List struct {
	l *list.List
}

func (list *List) Header() *list.Element {
	return list.l.Front()
}

func (list *List) Tail() *list.Element {
	return list.l.Back()
}

func NewRedisList() *List {
	return &List{l: list.New()}
}

func (list *List) LeftPush(data any) bool {
	list.l.PushFront(data)
	return true
}

func (list *List) RightPush(data any) bool {
	list.l.PushBack(data)
	return true
}

func (list *List) LeftPop() bool {
	if list.Len() == 0 {
		return false
	}
	list.l.Remove(list.l.Front())
	return true
}

func (list *List) RightPop() bool {
	if list.Len() == 0 {
		return false
	}
	list.l.Remove(list.l.Back())
	return true
}

func (list *List) IndexValue(index int) any {
	if index > list.Len()-1 || index < 0 {
		return nil
	}
	nowNode := list.l.Front()
	for i := 0; i < index; i++ {
		nowNode = nowNode.Next()
	}
	return nowNode.Value
}

func (list *List) Len() int {
	return list.l.Len()
}

func (list *List) Range(start int, end int) []any {
	var elements []any
	if end > list.Len()-1 {
		end = list.Len() - 1
	}

	if start < 0 {
		return nil
	}
	if start > end {
		return nil
	}

	nowNode := list.l.Front()
	for i := 0; i < start; i++ {
		nowNode = nowNode.Next()
	}
	for i := 0; i < end-start; i++ {
		elements = append(elements, nowNode.Value)
		nowNode = nowNode.Next()
	}
	return elements
}

func (list *List) InsertAfter(pivot, value any) bool {
	nowNode := list.l.Front()
	for i := 0; i < list.Len(); i++ {
		if trans.AnyCompare(nowNode.Value, pivot) {
			list.l.InsertAfter(value, nowNode)
			return true
		}
		nowNode = nowNode.Next()
	}
	return false
}

func (list *List) InsertBefore(pivot, value any) bool {
	nowNode := list.l.Front()
	for i := 0; i < list.Len(); i++ {
		if trans.AnyCompare(nowNode.Value, pivot) {
			list.l.InsertBefore(value, nowNode)
			return true
		}
		nowNode = nowNode.Next()
	}
	return false
}

func (list *List) Trim(start, end int) bool {
	if end > list.Len()-1 {
		end = list.Len() - 1
	}

	if start < 0 {
		return false
	}
	if start > end {
		return false
	}

	for i := 0; i < start; i++ {
		list.LeftPop()
	}
	for i := 0; i < list.Len()-end; i++ {
		list.RightPop()
	}
	return true
}

func (list *List) Set(index int, val any) bool {
	if index < 0 && index > list.Len()-1 {
		return false
	}
	nowNode := list.l.Front()
	for i := 0; i < index; i++ {
		nowNode = nowNode.Next()
	}
	if nowNode.Value == val {
		return false
	}
	nowNode.Value = val
	return true
}

func (list *List) Remove(val any) bool {
	nowNode := list.l.Front()
	for i := 0; i < list.Len(); i++ {
		if trans.AnyCompare(nowNode.Value, val) {
			list.l.Remove(nowNode)
			return true
		}
		nowNode = nowNode.Next()
	}
	return false
}
