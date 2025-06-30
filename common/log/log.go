package log

import (
	"fmt"
	"os"
	"time"

	gc "github.com/linuxsmiths/goncurses"
	"github.com/sirupsen/logrus"
)

var (
	// Our logger instance.
	stlog *logrus.Logger
)

// Returns true if we are running in debug mode.
func IsDebug() bool {
	return (os.Getenv("ST_DEBUG") == "1")
}

// Undo ncurses initialization and restore terminal state before terminating
// logs like Assert() and Fatalf().
func EndCurses() {
	gc.End()
	fmt.Printf("\033[?1003l\n")
}

func init() {
	// Create a new logger instance.
	stlog = logrus.New()

	stlog.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
		DisableColors:   false,
	})

	// More verbose logging for debug builds.
	if IsDebug() {
		stlog.SetLevel(logrus.TraceLevel)
	} else {
		stlog.SetLevel(logrus.InfoLevel)
	}

	// Include file name and line number in logs.
	stlog.SetReportCaller(true)
}

func Tracef(format string, args ...interface{}) {
	stlog.Tracef(format, args...)
}

func Debugf(format string, args ...interface{}) {
	stlog.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	stlog.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	stlog.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	stlog.Errorf(format, args...)
}

func Panicf(format string, args ...interface{}) {
	stlog.Panicf(format, args...)
}

// Fatalf logs a message and causes the process to exit with a stack trace and
// an exit code of 1. This is typically used for unrecoverable errors.
func Fatalf(format string, args ...interface{}) {
	// Restore terminal to show fatal log.
	EndCurses()
	stlog.Fatalf(format, args...)
}

// Use Assert for asserting for invariants. It causes the program to terminate
// along with a useful stack trace and an error message if the condition is
// false. In addition to the condition, it can take a variable number of
// arguments to provide additional context about the assertion failure.
// Disabled in non-debug builds.
//
// Examples:
//
// common.Assert(false)
// common.Assert(err == nil, err)
// common.Assert((value >= 0 && value <= 100), "Invalid percentage", value)
func Assert(cond bool, msg ...interface{}) {
	if !IsDebug() || cond {
		return
	}

	// Reset the terminal so as the assert backtrace shows up properly.
	EndCurses()

	if len(msg) != 0 {
		Panicf("Assertion failed: %v", msg)
	} else {
		Panicf("Assertion failed!")
	}
}
