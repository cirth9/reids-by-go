package redis_list

import (
	"container/list"
	"testing"
)

func TestList(t *testing.T) {
	l := list.New()
	t.Log(l.Len())
	l.Remove(l.Front())
	
}
