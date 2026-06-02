package handlers

import (
	"testing"
	"time"
)

func TestStatusClass(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"COMPLETED", "cq-badge cq-badge-good"},
		{"ERRORED", "cq-badge border-red-500/25 bg-red-500/10 text-red-300"},
		{"RUNNING", "cq-badge border-sky-500/25 bg-sky-500/10 text-sky-300"},
		{"unknown", "cq-badge cq-badge-muted"},
	}
	for _, c := range cases {
		got := StatusClass(c.input)
		if got != c.want {
			t.Errorf("StatusClass(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestShortenID(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"short", "short"},
		{"exactly12chars", "exactly12cha"},
		{"", ""},
	}
	for _, c := range cases {
		got := shortenID(c.input)
		if got != c.want {
			t.Errorf("shortenID(%q) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestIsDLQ(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"my-queue-dlq", true},
		{"my_queue_dlq", true},
		{"normal-queue", false},
		{"dlq-prefix", false},
		{"dlq", false},
	}
	for _, c := range cases {
		got := isDLQ(c.input)
		if got != c.want {
			t.Errorf("isDLQ(%q) = %v, want %v", c.input, got, c.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{45 * time.Second, "45s"},
		{2 * time.Minute, "2m"},
		{90 * time.Second, "1m"},
		{2*time.Hour + 5*time.Minute, "2h 5m"},
	}
	for _, c := range cases {
		got := formatDuration(c.d)
		if got != c.want {
			t.Errorf("formatDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}
