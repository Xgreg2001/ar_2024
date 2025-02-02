package logger

import (
	"io"
	"log"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// FilteredLogger wraps an hclog.Logger to filter certain messages
type FilteredLogger struct {
	logger hclog.Logger
}

// New creates a new FilteredLogger
func New(logger hclog.Logger) *FilteredLogger {
	return &FilteredLogger{logger: logger}
}

func (f *FilteredLogger) Name() string { return f.logger.Name() }

func (f *FilteredLogger) Trace(msg string, args ...interface{}) {}
func (f *FilteredLogger) Debug(msg string, args ...interface{}) {
	// Filter out heartbeat messages
	if !strings.Contains(msg, "failed to heartbeat") &&
		!strings.Contains(msg, "failed to contact") &&
		!strings.Contains(msg, "failed to appendEntries") {
		f.logger.Debug(msg, args...)
	}
}
func (f *FilteredLogger) Info(msg string, args ...interface{}) { f.logger.Info(msg, args...) }
func (f *FilteredLogger) Warn(msg string, args ...interface{}) { f.logger.Warn(msg, args...) }
func (f *FilteredLogger) Error(msg string, args ...interface{}) {
	// Filter out timeout-related error messages
	if !strings.Contains(msg, "failed to appendEntries") &&
		!strings.Contains(msg, "failed to heartbeat") &&
		!strings.Contains(msg, "send timed out") {
		f.logger.Error(msg, args...)
	}
}

func (f *FilteredLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	switch level {
	case hclog.Trace:
		f.Trace(msg, args...)
	case hclog.Debug:
		f.Debug(msg, args...)
	case hclog.Info:
		f.Info(msg, args...)
	case hclog.Warn:
		f.Warn(msg, args...)
	case hclog.Error:
		f.Error(msg, args...)
	}
}

func (f *FilteredLogger) ImpliedArgs() []interface{} { return f.logger.ImpliedArgs() }
func (f *FilteredLogger) IsTrace() bool              { return f.logger.IsTrace() }
func (f *FilteredLogger) IsDebug() bool              { return f.logger.IsDebug() }
func (f *FilteredLogger) IsInfo() bool               { return f.logger.IsInfo() }
func (f *FilteredLogger) IsWarn() bool               { return f.logger.IsWarn() }
func (f *FilteredLogger) IsError() bool              { return f.logger.IsError() }
func (f *FilteredLogger) With(args ...interface{}) hclog.Logger {
	return &FilteredLogger{logger: f.logger.With(args...)}
}
func (f *FilteredLogger) Named(name string) hclog.Logger {
	return &FilteredLogger{logger: f.logger.Named(name)}
}
func (f *FilteredLogger) ResetNamed(name string) hclog.Logger {
	return &FilteredLogger{logger: f.logger.ResetNamed(name)}
}
func (f *FilteredLogger) SetLevel(level hclog.Level) {}
func (f *FilteredLogger) GetLevel() hclog.Level      { return f.logger.GetLevel() }
func (f *FilteredLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return f.logger.StandardLogger(opts)
}
func (f *FilteredLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return f.logger.StandardWriter(opts)
}
