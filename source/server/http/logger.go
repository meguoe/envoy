package httpserver

// logger.go —— HTTP 日志配置
//
// 通过标准库 log/slog 输出文本或 JSON 日志。

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

var logLevelVar = new(slog.LevelVar)

func init() {
	logLevelVar.Set(slog.LevelInfo)
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevelVar})))
}

// ConfigureLogging 配置 HTTP 日志级别和输出格式。
func ConfigureLogging(level string, jsonLog bool) {
	SetLogLevel(level)
	opts := &slog.HandlerOptions{Level: logLevelVar}
	if jsonLog {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, opts)))
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, opts)))
}

// SetLogLevel 设置全局日志级别，支持 DEBUG、INFO、WARN、ERROR。
func SetLogLevel(v string) {
	logLevelVar.Set(parseLogLevel(v))
}

func parseLogLevel(v string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func logAttrs(level slog.Level, msg string, attrs ...any) {
	slog.Log(context.Background(), level, msg, attrs...)
}
