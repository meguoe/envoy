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

type slog struct {
	Level   string `json:"level"`
	Msg     string `json:"msg"`
	Time    string `json:"time"`
	Attrs   any    `json:"attrs,omitempty"`
}

var slogWriter io.Writer = os.Stderr
var slogEnabled atomic.Int32 // 0=off, 1=on

func EnableStructuredLogging(on bool) {
	if on {
		slogEnabled.Store(1)
	} else {
		slogEnabled.Store(0)
	}
}

func IsStructuredLogging() bool {
	return slogEnabled.Load() == 1
}

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
	b, _ := json.Marshal(s)
	b = append(b, '\n')
	slogWriter.Write(b)
}

func slogInfo(msg string, attrs map[string]any) { slogPrint("INFO", msg, attrs) }
func slogWarn(msg string, attrs map[string]any) { slogPrint("WARN", msg, attrs) }
func slogError(msg string, attrs map[string]any) { slogPrint("ERROR", msg, attrs) }
func slogDebug(msg string, attrs map[string]any) { slogPrint("DEBUG", msg, attrs) }
