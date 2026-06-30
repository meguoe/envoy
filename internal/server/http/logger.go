package httpserver

// logger.go —— 轻量分级日志
//
// 通过 config.yaml 的 log_level 控制输出级别: DEBUG, INFO, WARN, ERROR。默认 INFO。

import (
	"fmt"
	"log"
	"strings"
	"sync/atomic"
)

type logLevel int32

const (
	levelDebug logLevel = iota
	levelInfo
	levelWarn
	levelError
)

var currentLogLevel atomic.Int32

func init() {
	currentLogLevel.Store(int32(levelInfo))
}

func SetLogLevel(v string) {
	currentLogLevel.Store(int32(parseLogLevel(v)))
}

func parseLogLevel(v string) logLevel {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "DEBUG":
		return levelDebug
	case "WARN", "WARNING":
		return levelWarn
	case "ERROR":
		return levelError
	default:
		return levelInfo
	}
}

func logDebug(format string, args ...any) {
	logWithLevel(levelDebug, "DEBUG", format, args...)
}

func logInfo(format string, args ...any) {
	logWithLevel(levelInfo, "INFO", format, args...)
}

func logWarn(format string, args ...any) {
	logWithLevel(levelWarn, "WARN", format, args...)
}

func logError(format string, args ...any) {
	logWithLevel(levelError, "ERROR", format, args...)
}

func logWithLevel(level logLevel, name, format string, args ...any) {
	if level < logLevel(currentLogLevel.Load()) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if IsStructuredLogging() {
		switch name {
		case "DEBUG":
			slogDebug(msg, nil)
		case "INFO":
			slogInfo(msg, nil)
		case "WARN":
			slogWarn(msg, nil)
		case "ERROR":
			slogError(msg, nil)
		}
		return
	}
	log.Print("[" + name + "] " + msg)
}
