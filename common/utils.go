package common

import (
	"fmt"
	"github.com/stormgo/common/log"
	"os"
	"path/filepath"
	"unicode/utf8"
)

var (
	StormgoDir string // Directory where Stormgo is installed.
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

func GetStormgoDir() string {
	// Must be set.
	log.Assert(StormgoDir != "")

	return StormgoDir
}

func init() {
	ex, err := os.Executable()
	if err != nil {
		log.Fatalf("Error getting executable path: %v", err)
	}

	buildDir := filepath.Dir(ex)
	log.Infof("Running from dir %s", buildDir)

	// Executable path is stormgo/build/terminal.
	StormgoDir = filepath.Join(buildDir, "..")

	// Convert into a canonical path.
	StormgoDir, err := filepath.EvalSymlinks(StormgoDir)
	if err != nil {
		log.Fatalf("Error resolving symlinks from %s: %v", err)
	}

	StormgoDir, err = filepath.Abs(StormgoDir)
	if err != nil {
		log.Fatalf("Error getting absolute path from %s: %v", err)
	}

	StormgoDir = filepath.Clean(StormgoDir)
	log.Infof("StormgoDir is %s", StormgoDir)
}
