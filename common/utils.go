package common

import (
	"github.com/stormgo/common/log"
	"os"
)

// Use this to find out if we are running in a debug environment.
// There are some extra checks that we might want to do in debug builds
func IsDebugBuild() bool {
	return (os.Getenv("ST_DEBUG") == "1")
}

// Assert can be used to assert any condition. It'll cause the program to terminate.
// Apart from the assertion condition it takes a variable number of items to print, which would
// mostly be a message and/or err variable and optionally one or more relevant variables.
// In non-debug builds it's a no-op.
//
// Examples:
//
//	common.Assert(err == nil, "Unexpected error return", err)
//	common.Assert((value >= 0 && value <= 100), "Invalid percentage", value)
//	common.Assert(false)
func Assert(cond bool, msg ...interface{}) {
	if !IsDebugBuild() {
		return
	}

	if !cond {
		if len(msg) != 0 {
			log.Panicf("Assertion failed: %v", msg)
		} else {
			log.Panicf("Assertion failed!")
		}
	}
}
