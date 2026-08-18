package version

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfoPreservesReleaseMetadataExactly(t *testing.T) {
	previousVersion, previousCommit, previousBuildDate := Version, GitCommit, BuildDate
	t.Cleanup(func() {
		Version, GitCommit, BuildDate = previousVersion, previousCommit, previousBuildDate
	})

	Version = "2.0.0-rc.1"
	GitCommit = "0123456789abcdef0123456789abcdef01234567"
	BuildDate = "2026-08-17T12:34:56Z"

	assert.Equal(t, `ChronoQueue v2.0.0-rc.1
  Git Commit: 0123456789abcdef0123456789abcdef01234567
  Built:      2026-08-17T12:34:56Z`, Info())
}
