package utils

func BytesToStrings(bytes [][]byte) []string {
	result := make([]string, len(bytes))
	for i := 0; i < len(bytes); i++ {
		result = append(result, string(bytes[i]))
	}
	return result
}
