package log

import (
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	// Our logger instance.
	stlog *logrus.Logger
)

// Note: This is duplicated here from common/utils.go to avoid import loop.
func IsDebugBuild() bool {
	return (os.Getenv("ST_DEBUG") == "1")
}

func init() {
	// Allocate the logger.
	stlog = logrus.New()

	stlog.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339Nano,
		DisableColors:   false,
	})

	// More verbose logging for debug builds.
	if IsDebugBuild() {
		stlog.SetLevel(logrus.TraceLevel)
	} else {
		// Only log info or above for prod.
		stlog.SetLevel(logrus.InfoLevel)
	}

	// Include file name and line number in logs, if selected.
	stlog.SetReportCaller(true)
}

// Tracef logs a message at level Trace on the standard logger.
func Tracef(format string, args ...interface{}) {
	stlog.Tracef(format, args...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, args ...interface{}) {
	stlog.Debugf(format, args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, args ...interface{}) {
	stlog.Infof(format, args...)
}

// Warnf logs a message at level Warn on the standard logger.
func Warnf(format string, args ...interface{}) {
	stlog.Warnf(format, args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, args ...interface{}) {
	stlog.Errorf(format, args...)
}

// Panicf logs a message at level Panic on the standard logger.
func Panicf(format string, args ...interface{}) {
	stlog.Panicf(format, args...)
}

// Fatalf logs a message at level Fatal on the standard logger then the process will exit
// with status set to 1.
func Fatalf(format string, args ...interface{}) {
	stlog.Fatalf(format, args...)
}
