package common

import (
	"unicode/utf8"
)

func TruncateUTF8String(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}

	var size, x int
	for i := 0; i < maxRunes && x < len(s); i++ {
		_, size = utf8.DecodeRuneInString(s[x:])
		x += size
	}

	return s[:x]
}
