package utils

import (
	"reids-by-go/utils/trans"
	"testing"
)

func TestCompare(t *testing.T) {
	println(trans.AnyCompare(1, 1))
	println(trans.AnyCompare(1, nil))
	println(trans.AnyCompare(1, 1))
	println(trans.AnyCompare(1, 1))
}
