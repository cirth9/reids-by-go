package trans

import (
	"bytes"
	"log"
	"reflect"
	"strconv"
)

func BytesToStrings(bytesS [][]byte) []string {
	result := make([]string, 0)
	for i := 0; i < len(bytesS); i++ {
		result = append(result, string(bytes.TrimSuffix(bytesS[i], []byte{'\r', '\n'})))
	}
	return result
}

func StringsToBytes(strings []string) [][]byte {
	result := make([][]byte, 0)
	for _, s := range strings {
		result = append(result, []byte(s))
	}
	return result
}

func MapToBytes(m map[string]any) [][]byte {
	result := make([][]byte, 0)
	for key, value := range m {
		result = append(result, []byte(key))
		result = append(result, AnyToBytes(value))
	}
	return result
}

func AnyToBytes(a any) []byte {
	return []byte(AnyToString(a))
}

func AnyToString(a any) string {
	var result string
	switch a.(type) {
	case int, int8, int16, int32, int64:
		result = strconv.FormatInt(a.(int64), 10)
	case float32, float64:
		result = strconv.FormatFloat(a.(float64), 'g', -1, 64)
	case string:
		result = a.(string)
	}
	return result
}

func AnysToBytes(anys []any) [][]byte {
	result := make([][]byte, 0)
	for _, a := range anys {
		log.Println(AnyToString(a))
		result = append(result, AnyToBytes(a))
	}
	return result
}

func AnysToStrings(anys []any) []string {
	result := make([]string, 0)
	for _, a := range anys {
		result = append(result, AnyToString(a))
	}
	return result
}

func AnyCompare(v1 any, v2 any) bool {
	if v1 == nil && v2 == nil {
		return true
	} else if v1 == nil || v2 == nil {
		return false
	}
	reflectV1 := reflect.ValueOf(v1)
	reflectV2 := reflect.ValueOf(v2)
	if reflectV1.Type() != reflectV2.Type() {
		return false
	}
	return reflectV1 == reflectV2
}
