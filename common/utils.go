package common

import (
	"fmt"
	"unicode/utf8"
)

// If the string is longer than maxRunes, it truncates the string to maxRunes
// runes, else it pads the string with spaces to maxRunes runes. In either
// case, it returns a string that is exactly maxRunes runes long.
func TruncateAndPadUTF8String(s string, maxRunes int) string {
	paddedString := fmt.Sprintf("%*s", maxRunes, s)

	if utf8.RuneCountInString(paddedString) <= maxRunes {
		return paddedString
	}

	var size, x int
	for i := 0; i < maxRunes && x < len(paddedString); i++ {
		_, size = utf8.DecodeRuneInString(paddedString[x:])
		x += size
	}

	return paddedString[:x]
}
