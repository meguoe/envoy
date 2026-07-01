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

// init 初始化日志级别为 INFO。
func init() {
	currentLogLevel.Store(int32(levelInfo))
}

// SetLogLevel 设置全局日志级别，支持 DEBUG、INFO、WARN、ERROR。
func SetLogLevel(v string) {
	currentLogLevel.Store(int32(parseLogLevel(v)))
}

// parseLogLevel 将字符串日志级别转换为 logLevel 常量，不区分大小写。
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

// logDebug 输出 DEBUG 级别日志，仅在日志级别为 DEBUG 时生效。
func logDebug(format string, args ...any) {
	logWithLevel(levelDebug, "DEBUG", format, args...)
}

// logInfo 输出 INFO 级别日志。
func logInfo(format string, args ...any) {
	logWithLevel(levelInfo, "INFO", format, args...)
}

// logWarn 输出 WARN 级别日志。
func logWarn(format string, args ...any) {
	logWithLevel(levelWarn, "WARN", format, args...)
}

// logError 输出 ERROR 级别日志。
func logError(format string, args ...any) {
	logWithLevel(levelError, "ERROR", format, args...)
}

// logWithLevel 根据日志级别过滤输出，支持普通文本和结构化 JSON 两种模式。
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
