package httpserver

// slogger.go —— 结构化日志
//
// 替代 log.Printf，输出 JSON 格式便于 ELK/Loki 聚合。
// 保留原有分级日志接口（logDebug/logInfo/logWarn/logError）。

import (
	"encoding/json"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// slog 表示一条结构化日志记录。
type slog struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	Time  string `json:"time"`
	Attrs any    `json:"attrs,omitempty"`
}

var slogWriter io.Writer = os.Stderr
var slogEnabled atomic.Int32 // 0=off, 1=on

// EnableStructuredLogging 启用或禁用 JSON 结构化日志输出。
func EnableStructuredLogging(on bool) {
	if on {
		slogEnabled.Store(1)
	} else {
		slogEnabled.Store(0)
	}
}

// IsStructuredLogging 返回当前是否启用了结构化日志。
func IsStructuredLogging() bool {
	return slogEnabled.Load() == 1
}

// slogPrint 将日志记录序列化为 JSON 并写入输出。
func slogPrint(level, msg string, attrs map[string]any) {
	if slogEnabled.Load() == 0 {
		return
	}
	s := slog{
		Level: level,
		Msg:   msg,
		Time:  time.Now().UTC().Format(time.RFC3339Nano),
		Attrs: attrs,
	}
	b, err := json.Marshal(s)
	if err != nil {
		return
	}
	b = append(b, '\n')
	slogWriter.Write(b)
}

// slogInfo 输出 INFO 级别的结构化日志。
func slogInfo(msg string, attrs map[string]any)  { slogPrint("INFO", msg, attrs) }
// slogWarn 输出 WARN 级别的结构化日志。
func slogWarn(msg string, attrs map[string]any)  { slogPrint("WARN", msg, attrs) }
// slogError 输出 ERROR 级别的结构化日志。
func slogError(msg string, attrs map[string]any) { slogPrint("ERROR", msg, attrs) }
// slogDebug 输出 DEBUG 级别的结构化日志。
func slogDebug(msg string, attrs map[string]any) { slogPrint("DEBUG", msg, attrs) }
