package common

import (
	"fmt"
	"github.com/stormgo/common/log"
	"unicode/utf8"
)

func GetCopyright() []string {
	cs := []string{}

	cs = append(cs,
		"Stormgo terminal ver 0.1.0 - Copyright (c) 2025 Nagendra S Tomar, Stormgo authors")
	cs = append(cs,
		"Released under GNU GPLv2")

	return cs
}

// If the string is longer than maxRunes, it truncates the string to maxRunes
// runes, else it pads the string with spaces to maxRunes runes. In either
// case, it returns a string that is exactly maxRunes runes long.
func TruncateAndPadUTF8String(s string, maxRunes int) string {
	paddedString := fmt.Sprintf("%*s", maxRunes, s)

	runeCount := utf8.RuneCountInString(paddedString)
	log.Assert(runeCount >= maxRunes, runeCount, maxRunes)
	//
	// We don't expect non-ascii characters to be present in the padded
	// string. If we need to support non-ascii characters, we should extend
	// the goncurses package.
	//
	log.Assert(runeCount == len(paddedString), runeCount, len(paddedString), paddedString)

	if runeCount == maxRunes {
		return paddedString
	}

	var size, x int
	for i := 0; i < maxRunes && x < len(paddedString); i++ {
		_, size = utf8.DecodeRuneInString(paddedString[x:])
		x += size
	}

	return paddedString[:x]
}
