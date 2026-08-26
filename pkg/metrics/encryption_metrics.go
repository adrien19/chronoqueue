package metrics

import "github.com/prometheus/client_golang/prometheus"

var encryptionKeyRefreshFailures = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "chronoqueue_encryption_key_refresh_failures_total",
		Help: "Total number of encryption key refresh failures",
	},
)

func IncrementEncryptionKeyRefreshFailures() {
	encryptionKeyRefreshFailures.Inc()
}
