package version

import (
	"fmt"
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
	return fmt.Sprintf(`ChronoQueue v%s
  Git Commit: %s
  Built:      %s`, Version, GitCommit, BuildDate)
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
