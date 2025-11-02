package version

import (
	"fmt"
	"time"
)

// These variables are set at build time via ldflags
var (
	// Version is the semantic version of ChronoQueue
	Version = "dev"
	// GitCommit is the git commit hash
	GitCommit = "unknown"
	// BuildDate is the date the binary was built
	BuildDate = "unknown"
)

// Info returns a user-friendly formatted version string
func Info() string {
	buildDate := formatBuildDate(BuildDate)
	commitShort := GitCommit
	if len(commitShort) > 7 {
		commitShort = commitShort[:7]
	}

	return fmt.Sprintf(`ChronoQueue v%s
  Git Commit: %s
  Built:      %s`, Version, commitShort, buildDate)
}

// Short returns just the version number
func Short() string {
	return Version
}

// Details returns a structured version information
func Details() map[string]string {
	return map[string]string{
		"version":    Version,
		"git_commit": GitCommit,
		"build_date": BuildDate,
	}
}

// formatBuildDate converts ISO 8601 timestamp to a more readable format
func formatBuildDate(dateStr string) string {
	// Try to parse the ISO 8601 format: 2025-11-02T23:20:16Z
	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		// If parsing fails, return the original string
		return dateStr
	}

	// Format as: November 2, 2025 at 23:20 UTC
	return t.Format("January 2, 2006 at 15:04 MST")
}
