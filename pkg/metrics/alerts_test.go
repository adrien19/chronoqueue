package metrics

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPrometheusAlertsReferenceRegisteredMetrics(t *testing.T) {
	alertBytes, err := os.ReadFile(filepath.Join("..", "..", "monitoring", "prometheus-alerts.yml"))
	require.NoError(t, err)
	var rules struct {
		Groups []struct {
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	require.NoError(t, yaml.Unmarshal(alertBytes, &rules))

	registered := make(map[string]struct{})
	namePattern := regexp.MustCompile(`Name:\s*"(chronoqueue_[a-z0-9_]+)"`)
	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, source := range sources {
		contents, err := os.ReadFile(source)
		require.NoError(t, err)
		for _, match := range namePattern.FindAllSubmatch(contents, -1) {
			registered[string(match[1])] = struct{}{}
		}

	}

	metricPattern := regexp.MustCompile(`chronoqueue_[a-z0-9_]+`)
	for _, group := range rules.Groups {
		for _, rule := range group.Rules {
			for _, metricName := range metricPattern.FindAllString(rule.Expr, -1) {
				baseName := strings.TrimSuffix(metricName, "_bucket")
				_, exists := registered[baseName]
				require.True(t, exists, "alert %s references unregistered metric %s", rule.Alert, metricName)
			}
		}
	}
}
