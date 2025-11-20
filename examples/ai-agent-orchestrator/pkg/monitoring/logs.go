package monitoring

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// LogLevel represents log severity
type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp time.Time
	Level     LogLevel
	Component string
	TaskID    string
	Message   string
}

// LogViewer provides log aggregation and viewing capabilities
type LogViewer struct {
	logPaths []string
}

// NewLogViewer creates a new log viewer
func NewLogViewer(logPaths ...string) *LogViewer {
	return &LogViewer{
		logPaths: logPaths,
	}
}

// TailLogs streams recent log entries
func (lv *LogViewer) TailLogs(ctx context.Context, lines int, filter string) error {
	for _, logPath := range lv.logPaths {
		if err := lv.tailSingleLog(ctx, logPath, lines, filter); err != nil {
			// Continue on error, just print warning
			fmt.Fprintf(os.Stderr, "Warning: could not read %s: %v\n", logPath, err)
		}
	}
	return nil
}

// tailSingleLog reads the last N lines from a log file
func (lv *LogViewer) tailSingleLog(ctx context.Context, logPath string, lines int, filter string) error {
	file, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close file: %v\n", closeErr)
		}
	}()

	// Read all lines into memory (simplified approach)
	var allLines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Apply filter if specified
		if filter == "" || strings.Contains(line, filter) {
			allLines = append(allLines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Print last N lines
	start := len(allLines) - lines
	if start < 0 {
		start = 0
	}

	fmt.Printf("\n📄 %s\n", logPath)
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")

	for i := start; i < len(allLines); i++ {
		fmt.Println(colorizeLogLine(allLines[i]))
	}

	return nil
}

// FollowLogs continuously streams new log entries
func (lv *LogViewer) FollowLogs(ctx context.Context, filter string) error {
	// For simplicity, we'll just tail and refresh
	// A real implementation would use file watching (fsnotify)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			fmt.Print("\033[H\033[2J") // Clear screen
			if err := lv.TailLogs(ctx, 50, filter); err != nil {
				return err
			}
		}
	}
}

// colorizeLogLine adds color to log lines based on level
func colorizeLogLine(line string) string {
	// Simple colorization based on log level keywords
	if strings.Contains(line, "ERROR") || strings.Contains(line, "FATAL") {
		return fmt.Sprintf("\033[31m%s\033[0m", line) // Red
	}
	if strings.Contains(line, "WARN") {
		return fmt.Sprintf("\033[33m%s\033[0m", line) // Yellow
	}
	if strings.Contains(line, "INFO") {
		return fmt.Sprintf("\033[32m%s\033[0m", line) // Green
	}
	return line
}

// DisplayLogSummary shows a summary of recent log activity
func (lv *LogViewer) DisplayLogSummary(ctx context.Context, since time.Duration) error {
	fmt.Println("\n╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Printf("║  LOG SUMMARY - Last %s%56s║\n", formatDuration(since), "")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Count log levels across all log files
	levelCounts := make(map[LogLevel]int)
	cutoff := time.Now().Add(-since)

	for _, logPath := range lv.logPaths {
		file, err := os.Open(logPath)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			// Simple timestamp extraction (assumes ISO 8601 format at start)
			if len(line) > 19 {
				timestamp, err := time.Parse("2006-01-02T15:04:05", line[:19])
				if err == nil && timestamp.After(cutoff) {
					// Count by level
					for _, level := range []LogLevel{LogLevelError, LogLevelWarn, LogLevelInfo, LogLevelDebug} {
						if strings.Contains(line, string(level)) {
							levelCounts[level]++
							break
						}
					}
				}
			}
		}
		if closeErr := file.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close file: %v\n", closeErr)
		}
	}

	// Display summary
	fmt.Println("📊 Log Level Distribution")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  ❌ ERROR:   %d\n", levelCounts[LogLevelError])
	fmt.Printf("  ⚠️  WARN:    %d\n", levelCounts[LogLevelWarn])
	fmt.Printf("  ℹ️  INFO:    %d\n", levelCounts[LogLevelInfo])
	fmt.Printf("  🔍 DEBUG:   %d\n", levelCounts[LogLevelDebug])
	fmt.Println()

	return nil
}
